//go:build !disable_clickhouse_provider

package db

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type Streamer interface {
	Append(row abstract.ChangeItem) error
	Finish() error // Finish commits all awaiting data and closes Streamer.
	Close() error
}

type MarshallingError interface {
	IsMarshallingError()
	error
}

func IsMarshallingError(err error) bool {
	var target MarshallingError
	return xerrors.As(err, &target)
}

type ChangeItemMarshaller func(item abstract.ChangeItem) ([]any, error)

type StreamInserter interface {
	StreamInsert(query string, marshaller ChangeItemMarshaller) (Streamer, error)
}
