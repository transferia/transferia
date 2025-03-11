package source

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/providers/s3"
	"github.com/transferria/transferria/pkg/providers/s3/reader"
	"go.ytsaurus.tech/library/go/core/log"
)

type ObjectFetcher interface {
	// FetchObjects derives a list of new objects that need replication from a configured source.
	// This can be a creation event messages from an SQS, SNS, Pub/Sub queue or directly by reading the full object list from the s3 bucket itself.
	FetchObjects() ([]Object, error)

	// Commit persist the processed object to some state.
	// For SQS it deletes the processed messages, for SNS/PubSub it Ack the processed messages
	// and for normal S3 bucket polling it stores the latest object that was read to the transfer state.
	Commit(Object) error
}

type Object struct {
	Name         string    `json:"name"`
	LastModified time.Time `json:"last_modified"`
}

func NewObjectFetcher(ctx context.Context, client s3iface.S3API, logger log.Logger,
	cp coordinator.Coordinator, transferID string, reader reader.Reader,
	sess *session.Session, sourceConfig *s3.S3Source,
) (ObjectFetcher, error) {
	if sourceConfig == nil {
		return nil, xerrors.New("missing configuration")
	}

	if sourceConfig.EventSource.SQS != nil && sourceConfig.EventSource.SQS.QueueName != "" {
		source, err := NewSQSSource(ctx, logger, reader, sess, sourceConfig)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new sqs source: %w", err)
		}
		return source, nil
	} else if sourceConfig.EventSource.SNS != nil {
		return nil, xerrors.New("not yet implemented SNS")
	} else if sourceConfig.EventSource.PubSub != nil {
		return nil, xerrors.New("not yet implemented PubSub")
	} else {
		// default to polling of object list
		source, err := NewPollingSource(ctx, client, logger, cp, transferID, reader, sourceConfig)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize polling source: %w", err)
		}
		return source, nil
	}
}
