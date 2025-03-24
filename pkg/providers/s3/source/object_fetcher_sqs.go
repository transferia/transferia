package source

import (
	"context"
	"encoding/json"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ ObjectFetcher = (*sqsSource)(nil)

type sqsSource struct {
	cfg         *s3.SQS
	client      *sqs.SQS
	ctx         context.Context
	queueURL    *string // string pointer is used here since the aws sdk expects/returns all data types as pointers
	reader      reader.Reader
	toDelete    []*sqs.DeleteMessageBatchRequestEntry          // unusable messages from the queue (different non-creation events, folder creation events...)
	inflight    map[string]*sqs.DeleteMessageBatchRequestEntry // inflight messages being processed, key is file name, value is ReceiptHandle of the message
	logger      log.Logger
	pathPattern string
	mu          sync.Mutex
}

// DTO struct is used for unmarshalling SQS messages in the FetchObjects method.
type DTO struct {
	Type    string `json:"Type"`
	Message string `json:"Message"`
	Records []struct {
		S3 struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key  string `json:"key"`
				Size int64  `json:"size"`
			} `json:"object"`
			ConfigurationID string `json:"configurationId"`
		} `json:"s3"`

		EventName string    `json:"eventName"`
		EventTime time.Time `json:"eventTime"`
	} `json:"Records"`
}

func (s *sqsSource) fetchMessages() ([]Object, error) {
	messages, err := s.client.ReceiveMessageWithContext(s.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            s.queueURL,
		MaxNumberOfMessages: aws.Int64(10),  // maximum is 10, but fewer  msg can be delivered
		WaitTimeSeconds:     aws.Int64(20),  // reduce cost by switching to long polling, 20s is max wait time
		VisibilityTimeout:   aws.Int64(600), // set read timeout to 10 min initially
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to fetch new messages from sqs queue: %w", err)
	}

	var objectList []Object
	for _, message := range messages.Messages {
		// all received messages should be deleted once they are processed
		currentMessage := &sqs.DeleteMessageBatchRequestEntry{
			Id:            message.MessageId,
			ReceiptHandle: message.ReceiptHandle,
		}
		if !strings.Contains(*message.Body, TestEvent) && strings.Contains(*message.Body, CreationEvent) {
			var dto DTO
			if err := json.Unmarshal([]byte(*message.Body), &dto); err != nil {
				return nil, xerrors.Errorf("failed to unmarshal message records: %w", err)
			}
			if len(dto.Records) == 0 && len(dto.Message) > 0 {
				// we receive wrapped message, need to unwrap it, actual records are inside `Message` field.
				if err := json.Unmarshal([]byte(dto.Message), &dto); err != nil {
					return nil, xerrors.Errorf("failed to unmarshal message records: %w", err)
				}
			}
			for _, record := range dto.Records {
				if strings.Contains(record.EventName, CreationEvent) {
					// SQS escapes path strings, we need to invert the operation here, from simple%3D1234.jsonl to simple=1234.jsonl for example
					unescapedKey, err := url.QueryUnescape(record.S3.Object.Key)
					if err != nil {
						return nil, xerrors.Errorf("failed to unescape S3 object key from SQS queue: %w", err)
					}
					if reader.SkipObject(&aws_s3.Object{
						Key:  aws.String(unescapedKey),
						Size: aws.Int64(record.S3.Object.Size),
					}, s.pathPattern, "|", s.reader.ObjectsFilter()) {
						s.logger.Infof("File did not pass type/path check, skipping: file %s, pathPattern: %s", unescapedKey, s.pathPattern)
						s.toDelete = append(s.toDelete, currentMessage) // most probably a folder creation event message
						continue
					}

					objectList = append(objectList, Object{
						Name:         unescapedKey,
						LastModified: record.EventTime,
					})
					s.mu.Lock()
					s.inflight[unescapedKey] = currentMessage
					s.mu.Unlock()
				} else {
					s.toDelete = append(s.toDelete, currentMessage) // update/delete event messages
				}
			}
		} else {
			s.logger.Infof("Retrieved non-creation event from SQS queue, event: %s", *message.Body)
			s.toDelete = append(s.toDelete, currentMessage) // test event messages and such
		}
	}

	sort.Slice(objectList, func(i, j int) bool {
		return objectList[i].LastModified.Before(objectList[j].LastModified)
	})

	return objectList, nil
}

func (s *sqsSource) FetchObjects() ([]Object, error) {
	var objectList []Object
	returnResults := false
	for {
		obj, err := s.fetchMessages()
		if err != nil {
			return nil, xerrors.Errorf("failed to read new messages form SQS: %w", err)
		}
		if len(obj) != 0 {
			objectList = append(objectList, obj...)
			returnResults = true
		}

		if len(obj) == 0 && len(s.toDelete) == 0 {
			// no new SQS messages, return and wait
			return objectList, nil
		}

		if err := s.batchDelete(); err != nil {
			return nil, xerrors.Errorf("failed to delete non-processable SQS messages: %w", err)
		}

		if returnResults {
			return objectList, nil
		}
	}
}

func (s *sqsSource) visibilityHeartbeat(errChan chan error) {
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Stopping run")
			return
		default:
		}

		// copy the map to avoid holding lock for to long
		inflightCopy := s.copyInflight()

		var batchOfTenMessages []*sqs.ChangeMessageVisibilityBatchRequestEntry
		for _, message := range inflightCopy {
			if len(batchOfTenMessages) == 10 {
				if err := s.sendBatchChangeVisibility(batchOfTenMessages); err != nil {
					s.logger.Errorf("updating message visibility failed: %v", err)
					util.Send(s.ctx, errChan, err)
					return
				}
				batchOfTenMessages = []*sqs.ChangeMessageVisibilityBatchRequestEntry{}
			}

			batchOfTenMessages = append(batchOfTenMessages, &sqs.ChangeMessageVisibilityBatchRequestEntry{
				Id:                message.Id,
				ReceiptHandle:     message.ReceiptHandle,
				VisibilityTimeout: aws.Int64(600), // reset visibility timeout again to 10 min
			})

		}
		if len(batchOfTenMessages) > 0 {
			// some messages still to update
			if err := s.sendBatchChangeVisibility(batchOfTenMessages); err != nil {
				s.logger.Errorf("updating message visibility failed: %v", err)
				util.Send(s.ctx, errChan, err)
				return
			}
		}

		time.Sleep(5 * time.Minute) // just to be safe sleep only 1/2 te time of the visibility timeout
	}
}

func (s *sqsSource) sendBatchChangeVisibility(toChange []*sqs.ChangeMessageVisibilityBatchRequestEntry) error {
	res, err := s.client.ChangeMessageVisibilityBatchWithContext(s.ctx, &sqs.ChangeMessageVisibilityBatchInput{
		Entries:  toChange,
		QueueUrl: s.queueURL,
	})
	if err != nil {
		return xerrors.Errorf("failed to increase messages visibility timeout: %w", err)
	}
	if len(res.Failed) > 0 {
		// check operations, only allowed to continue on ReceiptHandleIsInvalid operations
		for _, fail := range res.Failed {
			if *fail.Code == sqs.ErrCodeReceiptHandleIsInvalid {
				// happens if the message is deleted in the meantime
				s.logger.Warnf("Tried to increase visibility timeout on message %s, but message might have been deleted in the meantime: %s", *fail.Id, *fail.Message)
				continue
			} else {
				return xerrors.Errorf("failed to increase visibility timeout on message: %s, error: %s, errCode: %s", *fail.Id, *fail.Message, *fail.Code)
			}
		}
	}

	return nil
}

func (s *sqsSource) copyInflight() map[string]*sqs.DeleteMessageBatchRequestEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	inflightCopy := make(map[string]*sqs.DeleteMessageBatchRequestEntry)
	for key, val := range s.inflight {
		inflightCopy[key] = val
	}
	return inflightCopy
}

func fetchQueueURL(ctx context.Context, client *sqs.SQS, ownerAccountID, queueName string) (*string, error) {
	var accountID *string
	if ownerAccountID != "" {
		accountID = aws.String(ownerAccountID)
	}

	queueResult, err := client.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName:              aws.String(queueName),
		QueueOwnerAWSAccountId: accountID,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to fetch sqs queue url: %w", err)
	}
	return queueResult.QueueUrl, nil
}

func (s *sqsSource) Commit(object Object) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	receiptHandle := s.inflight[object.Name]
	if receiptHandle != nil {
		if _, err := s.client.DeleteMessageWithContext(s.ctx, &sqs.DeleteMessageInput{
			ReceiptHandle: receiptHandle.ReceiptHandle,
			QueueUrl:      s.queueURL,
		}); err != nil {
			return xerrors.Errorf("failed to delete processed message for file %s: %w", object.Name, err)
		}
		delete(s.inflight, object.Name)
	}

	return nil
}

func (s *sqsSource) batchDelete() error {
	if len(s.toDelete) > 0 {
		if _, err := s.client.DeleteMessageBatchWithContext(s.ctx, &sqs.DeleteMessageBatchInput{
			Entries:  s.toDelete,
			QueueUrl: s.queueURL,
		}); err != nil {
			return xerrors.Errorf("failed to batch delete processed messages: %w", err)
		}
		s.toDelete = []*sqs.DeleteMessageBatchRequestEntry{}
	}

	return nil
}

func NewSQSSource(ctx context.Context, logger log.Logger, reader reader.Reader,
	sess *session.Session, sourceConfig *s3.S3Source,
) (*sqsSource, error) {
	sqsConfig := sourceConfig.EventSource.SQS
	if sqsConfig == nil {
		return nil, xerrors.New("missing sqs configuration")
	}
	sqsSession := sess
	if sqsConfig.ConnectionConfig.AccessKey != "" {
		logger.Info("Using dedicated session for sqs client")
		s, err := s3.NewAWSSession(logger, sourceConfig.Bucket, sqsConfig.ConnectionConfig)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize session for sqs: %w", err)
		}
		sqsSession = s
	}

	client := sqs.New(sqsSession)

	queueURL, err := fetchQueueURL(ctx, client, sqsConfig.OwnerAccountID, sqsConfig.QueueName)
	if err != nil {
		return nil, xerrors.Errorf("failed to initialize sqs queue url: %w", err)
	}

	source := &sqsSource{
		client:      sqs.New(sqsSession),
		ctx:         ctx,
		cfg:         sqsConfig,
		queueURL:    queueURL,
		reader:      reader,
		logger:      logger,
		pathPattern: sourceConfig.PathPattern,
		toDelete:    []*sqs.DeleteMessageBatchRequestEntry{},
		inflight:    make(map[string]*sqs.DeleteMessageBatchRequestEntry),
		mu:          sync.Mutex{},
	}

	return source, nil
}
