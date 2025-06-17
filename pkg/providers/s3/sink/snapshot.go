//go:build !disable_s3_provider

package sink

import "github.com/transferia/transferia/pkg/serializer"

type Snapshot interface {
	Read(buf []byte) (n int, err error)
	FeedChannel() chan<- []byte
	Close()
}

type snapshotHolder struct {
	uploadDone chan error
	snapshot   Snapshot
	serializer serializer.BatchSerializer
}
