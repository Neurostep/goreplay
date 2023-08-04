package protoprovider

import (
	"errors"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
)

var protoPaths = make(map[string]*ProtoMethod)

// ProtoMethod represents method of grpc
type ProtoMethod struct {
	Method  *desc.MethodDescriptor
	Service *desc.ServiceDescriptor
}

// GetProtoByPath return ProtoMethod by path
func GetProtoByPath(path string) (*ProtoMethod, bool) {
	if protoMethod, ok := protoPaths[path]; ok {
		return protoMethod, true
	}

	return nil, false
}

// Init ...
func Init(importPaths string, protoFiles []string) error {
	if importPaths == "" || len(protoFiles) == 0 {
		return nil
	}

	fileNames, err := protoparse.ResolveFilenames([]string{importPaths}, protoFiles...)
	if err != nil {
		return err
	}
	p := protoparse.Parser{
		ImportPaths:           []string{importPaths},
		InferImportPaths:      len(importPaths) == 0,
		IncludeSourceCodeInfo: true,
	}
	parsedFiles, err := p.ParseFiles(fileNames...)
	if err != nil {
		return err
	}

	if len(parsedFiles) < 1 {
		return errors.New("Not found proto messages")
	}

	for _, parsedFile := range parsedFiles {
		for _, service := range parsedFile.GetServices() {
			for _, method := range service.GetMethods() {
				protoPaths["/"+method.GetService().GetFullyQualifiedName()+"/"+method.GetName()] = &ProtoMethod{
					Method:  method,
					Service: service,
				}
			}
		}
	}

	return nil
}
