package serializer

import (
	"github.com/transferia/transferia/pkg/abstract"
)

type Serializer interface {
	Serialize(item *abstract.ChangeItem) ([]byte, error)
}

type BatchSerializer interface {
	Serialize(items []*abstract.ChangeItem) ([]byte, error)
}

type StreamSerializer interface {
	Serialize(items []*abstract.ChangeItem) error
	Close() error
}
