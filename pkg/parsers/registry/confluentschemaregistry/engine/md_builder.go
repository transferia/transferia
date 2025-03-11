package engine

import (
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/schemaregistry/confluent"
)

type key struct {
	globalID    int
	messageName string
}

type val struct {
	md         *desc.MessageDescriptor
	recordName string
}

type mdBuilder struct {
	mutex     sync.Mutex
	msgIDToMD map[key]val
}

func (b *mdBuilder) toMD(schema *confluent.Schema, refs map[string]confluent.Schema, name string) (*desc.MessageDescriptor, string, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	msgID := key{
		globalID:    schema.ID,
		messageName: name,
	}

	if cachedMD, ok := b.msgIDToMD[msgID]; ok {
		return cachedMD.md, cachedMD.recordName, nil
	}

	protoSchema := dirtyPatch(schema.Schema)
	stubProto := "stub.proto"
	currProtoMap := protoMap(stubProto, protoSchema)
	for k, v := range refs {
		currProtoMap[k] = v.Schema
	}
	p := protoparse.Parser{
		Accessor: protoparse.FileContentsFromMap(currProtoMap),
	}
	fds, err := p.ParseFiles(stubProto)
	if err != nil {
		return nil, "", xerrors.Errorf("unable to parse files, err: %w", err)
	}
	if len(fds) != 1 {
		return nil, "", xerrors.Errorf("protoparse.Parser.ParseFiles() returned array where length!=1")
	}

	fd := fds[0]
	var md *desc.MessageDescriptor
	var recordName = name
	if recordName != "" {
		md = fd.FindMessage(recordName)
	}
	if md == nil {
		recordName = getRecordName(fd)
		md = fd.FindMessage(recordName)
	}

	b.msgIDToMD[msgID] = val{md: md, recordName: recordName}
	return md, recordName, nil
}

func newMDBuilder() *mdBuilder {
	return &mdBuilder{
		mutex:     sync.Mutex{},
		msgIDToMD: make(map[key]val),
	}
}
