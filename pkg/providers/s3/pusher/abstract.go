package pusher

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
)

type Pusher interface {
	IsEmpty() bool
	Push(ctx context.Context, chunk Chunk) error
	// Ack is used in the parsqueue pusher as a way of keeping the state of files currently being processed clean.
	// Ack has no effect in the sync pusher, here files are processed from start to finish before new ones are fetched so no state is needed.
	// Ack is called by the ack method of the parsqueue once a chunk is pushed.
	// It returns a bool that gives information if a file was fully processed and is done.
	// It errors out if more then one ack was called on the same chunk of data.
	Ack(chunk Chunk) (bool, error)
}

type Chunk struct {
	FilePath  string
	Completed bool
	Offset    any
	Size      int64
	Items     []abstract.ChangeItem
}

func NewChunk(filePath string, completed bool, offset any, size int64, items []abstract.ChangeItem) Chunk {
	return Chunk{
		FilePath:  filePath,
		Completed: completed,
		Offset:    offset,
		Size:      size,
		Items:     items,
	}
}
