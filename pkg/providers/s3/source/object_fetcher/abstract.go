package objectfetcher

import (
	"github.com/transferia/transferia/pkg/providers/s3/reader"
)

type ObjectFetcher interface {
	RunBackgroundThreads(errCh chan error)

	// FetchObjects derives a list of new objects that need replication from a configured source.
	// This can be a creation event messages from an SQS, SNS, Pub/Sub queue or directly by reading the full object list from the s3 bucket itself.
	FetchObjects(reader reader.Reader) ([]string, error)

	// Commit persist the processed object to some state.
	// For SQS it deletes the processed messages, for SNS/PubSub it Ack the processed messages
	// and for normal S3 bucket polling it stores the latest object that was read to the transfer state.
	Commit(fileName string) error

	// FetchAndCommitAll on REPLICATION_ONLY persist known files on activate stage, to on replication process only new files
	FetchAndCommitAll(reader reader.Reader) error
}
