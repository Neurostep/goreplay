package goreplay

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/buger/goreplay/protoprovider"
	"github.com/buger/goreplay/stream"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
)

// GrpcOutputConfig struct for holding gRPC output configuration
type GrpcOutputConfig struct {
	WorkersMin    int           `json:"output-http-workers-min"`
	WorkersMax    int           `json:"output-http-workers"`
	QueueLen      int           `json:"output-http-queue-len"`
	Timeout       time.Duration `json:"output-http-timeout"`
	WorkerTimeout time.Duration `json:"output-http-worker-timeout"`
	ImportPath    string        `json:"output-grpc-import-path"`
	ProtoPaths    []string      `json:"output-grpc-proto-path"`
	address       string
}

func (hoc *GrpcOutputConfig) Copy() *GrpcOutputConfig {
	return &GrpcOutputConfig{
		WorkersMin:    hoc.WorkersMin,
		WorkersMax:    hoc.WorkersMax,
		QueueLen:      hoc.QueueLen,
		Timeout:       hoc.Timeout,
		WorkerTimeout: hoc.WorkerTimeout,
	}
}

type GrpcOutput struct {
	address string
	config  *GrpcOutputConfig

	streams *stream.Streams

	paths *sync.Map

	activeWorkers int32
	client        *grpc.ClientConn
	stopWorker    chan struct{}
	queue         chan *Message
	stop          chan bool // Channel used only to indicate goroutine should shutdown
}

func NewGrpcOutput(address string, config *GrpcOutputConfig) PluginWriter {
	o := new(GrpcOutput)

	err := protoprovider.Init(config.ImportPath, config.ProtoPaths)
	if err != nil {
		log.Fatal(err)
	}

	newConfig := config.Copy()
	if newConfig.Timeout < time.Millisecond*100 {
		newConfig.Timeout = time.Second
	}
	if newConfig.WorkersMin <= 0 {
		newConfig.WorkersMin = 1
	}
	if newConfig.WorkersMin > 1000 {
		newConfig.WorkersMin = 1000
	}
	if newConfig.WorkersMax <= 0 {
		newConfig.WorkersMax = math.MaxInt32 // idealy so large
	}
	if newConfig.WorkersMax < newConfig.WorkersMin {
		newConfig.WorkersMax = newConfig.WorkersMin
	}
	if newConfig.QueueLen <= 0 {
		newConfig.QueueLen = 1000
	}
	if newConfig.WorkerTimeout <= 0 {
		newConfig.WorkerTimeout = time.Second * 2
	}
	newConfig.address = address

	o.config = newConfig
	o.stop = make(chan bool)

	o.queue = make(chan *Message, o.config.QueueLen)

	o.address = address
	o.streams = stream.NewStreams()

	o.paths = &sync.Map{}

	// it should not be buffered to avoid races
	o.stopWorker = make(chan struct{})

	cc, err := o.connect(address)
	if err != nil {
		log.Fatal(err)
	}

	o.client = cc
	o.activeWorkers += int32(o.config.WorkersMin)
	for i := 0; i < o.config.WorkersMin; i++ {
		go o.startWorker()
	}
	go o.workerMaster()

	return o
}

func (o *GrpcOutput) workerMaster() {
	var timer = time.NewTimer(o.config.WorkerTimeout)
	defer func() {
		// recover from panics caused by trying to send in
		// a closed chan(o.stopWorker)
		recover()
	}()
	defer timer.Stop()
	for {
		select {
		case <-o.stop:
			return
		default:
			<-timer.C
		}
		// rollback workers
	rollback:
		if atomic.LoadInt32(&o.activeWorkers) > int32(o.config.WorkersMin) && len(o.queue) < 1 {
			// close one worker
			o.stopWorker <- struct{}{}
			atomic.AddInt32(&o.activeWorkers, -1)
			goto rollback
		}
		timer.Reset(o.config.WorkerTimeout)
	}
}

func (o *GrpcOutput) startWorker() {
	for {
		select {
		case <-o.stopWorker:
			return
		case msg := <-o.queue:
			o.sendRequest(o.client, msg)
		}
	}
}

func (o *GrpcOutput) sendRequest(client *grpc.ClientConn, msg *Message) {
	if !isRequestPayload(msg.Meta) {
		return
	}

	buf := bytes.NewReader(msg.Data)
	framer := http2.NewFramer(io.Discard, buf)
	framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)

	for {
		frame, err := framer.ReadFrame()
		if err == io.EOF {
			return
		}

		if err != nil {
			Debug(1, fmt.Sprintf("[gRPC-OUTPUT] error when reading frame: %q", err))
			return
		}

		connKey := string(payloadID(msg.Meta))
		streamID := frame.Header().StreamID
		var s *stream.Stream

		switch frame := frame.(type) {
		case *http2.MetaHeadersFrame:
			metaHeaders := make(map[string]string)
			isTrailer := false
			for _, hf := range frame.Fields {
				metaHeaders[hf.Name] = hf.Value
				if hf.Name == ":path" {
					s = &stream.Stream{
						ID:          streamID,
						Path:        hf.Value,
						Type:        stream.RequestType,
						MetaHeaders: make(map[string]string),
					}

					o.streams.Add(
						connKey,
						s,
					)

					o.paths.Store(connKey, hf.Value)
				} else if hf.Name == ":status" {
					s = &stream.Stream{
						ID:          streamID,
						Type:        stream.ResponseType,
						MetaHeaders: make(map[string]string),
					}

					if path, ok := o.paths.Load(connKey); ok {
						s.Path = path.(string)
					}

					o.streams.Add(
						connKey,
						s,
					)
				} else if hf.Name == "grpc-status" {
					s, _ = o.streams.Get(connKey, streamID)
					isTrailer = true
				}
			}

			if s != nil {
				if isTrailer {
					for key, value := range metaHeaders {
						s.MetaHeaders[key] = value
					}
				} else {
					s.MetaHeaders = metaHeaders
				}
			}
		case *http2.DataFrame:
			s, _ := o.streams.Get(connKey, streamID)

			grpcMessage, err := Decode(s.Path, s.ID, frame, s.Type, &s.GrpcState)
			if err != nil {
				log.Println(err)
			}

			switch s.Type {
			case stream.RequestType:
				msgFactory := dynamic.NewMessageFactoryWithDefaults()
				req := msgFactory.NewMessage(grpcMessage.req.GetMessageDescriptor())

				err := grpcMessage.req.ConvertTo(req)
				if err != nil {
					Debug(0, fmt.Sprintf("[gRPC-OUTPUT] error when converting to message: %q", err))
					return
				}

				stub := grpcdynamic.NewStubWithMessageFactory(client, msgFactory)

				var respHeaders metadata.MD
				var respTrailers metadata.MD

				ctx, cancel := context.WithTimeout(context.Background(), o.config.Timeout)
				defer cancel()

				resp, err := stub.InvokeRpc(
					ctx,
					grpcMessage.method.Method,
					req,
					grpc.Trailer(&respTrailers),
					grpc.Header(&respHeaders))

				if err != nil {
					Debug(0, fmt.Sprintf("[gRPC-OUTPUT] error when invoking RPC: %q", err))
					return
				}
				Debug(2, fmt.Sprintf("[gRPC-OUTPUT] response: %q", resp))

			case stream.ResponseType:
				s.ResponseGrpcMessage = grpcMessage
			}
		}
	}
}

// PluginWrite writes message to this plugin
func (o *GrpcOutput) PluginWrite(msg *Message) (n int, err error) {
	if !isRequestPayload(msg.Meta) {
		return len(msg.Data), nil
	}

	select {
	case <-o.stop:
		return 0, ErrorStopped
	case o.queue <- msg:
	}

	if len(o.queue) > 0 {
		// try to start a new worker to serve
		if atomic.LoadInt32(&o.activeWorkers) < int32(o.config.WorkersMax) {
			go o.startWorker()
			atomic.AddInt32(&o.activeWorkers, 1)
		}
	}
	return len(msg.Data) + len(msg.Meta), nil
}

func (o *GrpcOutput) connect(address string) (conn *grpc.ClientConn, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err = grpc.DialContext(
		ctx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	return
}

func (o *GrpcOutput) String() string {
	return "gRPC output: " + o.config.address
}

// Close closes the data channel so that data
func (o *GrpcOutput) Close() error {
	close(o.stop)
	close(o.stopWorker)
	return nil
}

const (
	maxMsgSize = 1024 * 1024 * 1024
)

type grpcCall struct {
	method *protoprovider.ProtoMethod
	req    *dynamic.Message
}

func Decode(path string, streamID uint32, frame *http2.DataFrame, side int, state *stream.GrpcState) (*grpcCall, error) {
	var dataBuf bytes.Buffer
	buf := frame.Data()
	if len(buf) == 0 {
		return nil, nil
	}

	if state.IsPartialRead {
		buf = append(state.Buf, buf...)
	}

	length := int(binary.BigEndian.Uint32(buf[1:5]))

	compress := buf[0]

	if compress == 1 {
		Debug(1, "%d use compression, msg %q\n", streamID, buf[5:])
		return nil, nil
	}

	dataBuf.Write(buf[5:])

	if length > maxMsgSize || dataBuf.Len() > maxMsgSize {
		dataBuf.Truncate(0)
		return nil, nil
	}

	if length != dataBuf.Len() {
		state.IsPartialRead = true
		state.Buf = buf

		return nil, nil
	}

	state.IsPartialRead = false
	state.Buf = nil

	data := dataBuf.Bytes()

	defer func() {
		dataBuf.Truncate(0)
	}()

	if method, ok := protoprovider.GetProtoByPath(path); ok {
		switch side {
		case 1:
			msg := dynamic.NewMessage(method.Method.GetInputType())

			mm := method.Method.GetInputType().AsProto()
			err := proto.Unmarshal(data, mm)
			if err != nil {
				return nil, err
			}

			if err := msg.Unmarshal(data); err != nil {
				return nil, err
			}
			return &grpcCall{
				method: method,
				req:    msg,
			}, nil
		case 2:
			return nil, nil
		}
	}

	return nil, nil
}
