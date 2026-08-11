package web

import (
	"context"
	"fmt"
	"sync"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type ProtoRegistry struct {
	mu      sync.RWMutex
	types   map[string]protoreflect.MessageDescriptor
	sources map[string]string
}

func NewProtoRegistry() *ProtoRegistry {
	return &ProtoRegistry{
		types:   make(map[string]protoreflect.MessageDescriptor),
		sources: make(map[string]string),
	}
}

func (r *ProtoRegistry) LoadProto(filename, content string) ([]string, []ProtoFieldInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sources[filename] = content

	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(r.sources),
		},
	}
	files, err := compiler.Compile(context.Background(), filename)
	if err != nil {
		delete(r.sources, filename)
		return nil, nil, fmt.Errorf("parse proto: %w", err)
	}

	result := files.FindFileByPath(filename)
	if result == nil {
		delete(r.sources, filename)
		return nil, nil, fmt.Errorf("compiled file not found: %s", filename)
	}

	var names []string
	var fieldList []ProtoFieldInfo
	collectMessages(result, &names, &fieldList, &r.types)
	return names, fieldList, nil
}

func collectMessages(
	file linker.File,
	names *[]string,
	fields *[]ProtoFieldInfo,
	registry *map[string]protoreflect.MessageDescriptor,
) {
	msgs := file.Messages()
	for i := 0; i < msgs.Len(); i++ {
		md := msgs.Get(i)
		name := string(md.FullName())
		*names = append(*names, name)
		(*registry)[name] = md
		*fields = append(*fields, collectFieldInfos(md)...)
		recurseNested(md, names, fields, registry)
	}
}

func recurseNested(
	md protoreflect.MessageDescriptor,
	names *[]string,
	fields *[]ProtoFieldInfo,
	registry *map[string]protoreflect.MessageDescriptor,
) {
	nested := md.Messages()
	for i := 0; i < nested.Len(); i++ {
		nm := nested.Get(i)
		name := string(nm.FullName())
		*names = append(*names, name)
		(*registry)[name] = nm
		*fields = append(*fields, collectFieldInfos(nm)...)
		recurseNested(nm, names, fields, registry)
	}
}

func (r *ProtoRegistry) Detect(data []byte) (typeName string, jsonPayload string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for name, md := range r.types {
		dm := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(data, dm); err != nil {
			continue
		}
		if len(dm.GetUnknown()) >= len(data)/2 {
			continue
		}
		jsonBytes, err := protojson.Marshal(dm)
		if err != nil {
			continue
		}
		return name, string(jsonBytes)
	}
	return "", ""
}

func (r *ProtoRegistry) Types() []ProtoTypeSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]ProtoTypeSummary, 0, len(r.types))
	for name, md := range r.types {
		fields := collectFieldInfos(md)
		out = append(out, ProtoTypeSummary{Name: name, Fields: fields})
	}
	return out
}

func (r *ProtoRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.types)
}

func (r *ProtoRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.types = make(map[string]protoreflect.MessageDescriptor)
	r.sources = make(map[string]string)
}

type ProtoTypeSummary struct {
	Name   string           `json:"name"`
	Fields []ProtoFieldInfo `json:"fields"`
}

type ProtoFieldInfo struct {
	Name     string `json:"name"`
	Number   int    `json:"number"`
	Type     string `json:"type"`
	Repeated bool   `json:"repeated,omitempty"`
}

func collectFieldInfos(md protoreflect.MessageDescriptor) []ProtoFieldInfo {
	fds := md.Fields()
	out := make([]ProtoFieldInfo, 0, fds.Len())
	for i := 0; i < fds.Len(); i++ {
		fd := fds.Get(i)
		out = append(out, ProtoFieldInfo{
			Name:     string(fd.Name()),
			Number:   int(fd.Number()),
			Type:     fd.Kind().String(),
			Repeated: fd.IsList(),
		})
	}
	return out
}
