package writer

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
)

type Writer interface {
	Write(data string) error
	Close() error
}

type WriterConfig interface {
	IsMeteringWriter()
	Type() string
}

type WriterFactory func(schema, topic, sourceID string, cfg WriterConfig) (Writer, error)

var registeredWriters = map[string]WriterFactory{}

func Register(name string, factory WriterFactory) {
	registeredWriters[name] = factory
}

func New(typ, schema, topic, sourceID string, cfg WriterConfig) (Writer, error) {
	fctr, ok := registeredWriters[typ]
	if !ok {
		return nil, xerrors.Errorf("writer %s not registered", typ)
	}
	return fctr(schema, topic, sourceID, cfg)
}
