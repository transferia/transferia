package serializer

import (
	"bytes"
	"context"
	"io"

	"github.com/transferia/transferia/pkg/abstract"
)

type Serializer interface {
	Serialize(item *abstract.ChangeItem) ([]byte, error)
	SerializeWithSeparatorTo(item *abstract.ChangeItem, separator []byte, buf *bytes.Buffer) error
	Close() ([]byte, error)
}

type BatchSerializer interface {
	Serialize(items []*abstract.ChangeItem) ([]byte, error)
	SerializeAndWrite(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) (int, error)
	Close() ([]byte, error)
}

type StreamSerializer interface {
	Serialize(items []*abstract.ChangeItem) error
	Close() error
}
