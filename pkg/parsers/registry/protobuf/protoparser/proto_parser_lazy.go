package protoparser

import (
	"github.com/transferia/transferia/library/go/ptr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoscanner"
)

var _ parsers.LazyParser = (*lazyProtoParser)(nil)

type lazyProtoParser struct {
	parser     *ProtoParser
	sc         protoscanner.ProtoScanner
	iterSt     *iterState
	partition  *abstract.Partition
	rawMessage *parsers.Message
}

func (p *lazyProtoParser) Next() *abstract.ChangeItem {
	res := p.next()
	if res != nil {
		res.FillQueueMessageMeta(p.partition.Topic, int(p.partition.Partition), p.rawMessage.Offset, res.Counter-1)
	}
	return res
}

func (p *lazyProtoParser) next() *abstract.ChangeItem {
	if !p.sc.Scan() {
		if err := p.sc.Err(); err != nil {
			p.iterSt.IncrementCounter()
			return ptr.T(unparsedChangeItem(p.iterSt, p.sc.RawData(), err))
		}
		return nil
	}
	return ptr.T(p.parser.processScanned(p.iterSt, p.sc, uint64(p.iterSt.WriteTime.UnixNano())))
}
