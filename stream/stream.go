package stream

import "sync"

const (
	//RequestType ...
	RequestType = 1
	//ResponseType ...
	ResponseType = 2
)

// Stream ...
type Stream struct {
	ID                  uint32
	MetaHeaders         map[string]string
	Path                string
	Type                int
	GrpcState           GrpcState
	ResponseGrpcMessage interface{}
}

type GrpcState struct {
	IsPartialRead bool
	Buf           []byte
}

type Streams struct {
	collection map[string]map[uint32]*Stream
	mutex      sync.RWMutex
}

// NewStreams ...
func NewStreams() *Streams {
	return &Streams{
		collection: make(map[string]map[uint32]*Stream),
		mutex:      sync.RWMutex{},
	}
}

// Add ...
func (streams *Streams) Add(connectionKey string, stream *Stream) {
	streams.mutex.Lock()
	defer streams.mutex.Unlock()

	if _, ok := streams.collection[connectionKey]; !ok {
		streams.collection[connectionKey] = make(map[uint32]*Stream)
	}

	streams.collection[connectionKey][stream.ID] = stream
}

// Get ...
func (streams *Streams) Get(connectionKey string, streamID uint32) (*Stream, bool) {
	streams.mutex.RLock()
	defer streams.mutex.RUnlock()

	if _, ok := streams.collection[connectionKey][streamID]; !ok {
		return nil, false
	}

	return streams.collection[connectionKey][streamID], true
}
