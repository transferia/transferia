package source

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/sink/testutil"
	"github.com/transferia/transferia/pkg/providers/s3/source"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

var (
	sqsEndpoint  = fmt.Sprintf("http://localhost:%s", os.Getenv("SQS_PORT"))
	sqsUser      = "test_s3_replication_sqs_user"
	sqsKey       = "unused"
	sqsQueueName = "test_s3_replication_sqs_queue"
	sqsRegion    = "yandex"
	messageBody  = `{"Records":[{"eventTime":"2023-08-09T11:46:36.337Z","eventName":"ObjectCreated:Put","s3":{"configurationId":"NewObjectCreateEvent","bucket":{"name":"test_csv_replication"},"object":{"key":"%s/%s","size":627}}}]}`
)

type mockAsyncSink struct {
	mu   sync.Mutex
	read int
}

func (m *mockAsyncSink) Close() error { return nil }

func (m *mockAsyncSink) AsyncPush(items []abstract.ChangeItem) chan error {
	res := make(chan error, 1)
	m.mu.Lock()
	m.read += len(items)
	m.mu.Unlock()
	res <- nil
	return res
}

func (m *mockAsyncSink) getCurrentlyRead() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.read
}

func TestNativeS3PathsAreUnescaped(t *testing.T) {
	testCasePath := "thousands_of_csv_files"
	src := s3.PrepareCfg(t, "data7", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, src, src.PathPrefix)
	}

	time.Sleep(5 * time.Second)

	src.TableNamespace = "test"
	src.TableName = "data"
	src.InputFormat = model.ParsingFormatCSV
	src.WithDefaults()
	src.Format.CSVSetting.BlockSize = 10000000
	src.ReadBatchSize = 4000 // just for testing so its faster, normally much smaller
	src.Format.CSVSetting.QuoteChar = "\""

	src.EventSource.SQS = &s3.SQS{
		QueueName: sqsQueueName,
		ConnectionConfig: s3.ConnectionConfig{
			AccessKey: sqsUser,
			SecretKey: model.SecretString(sqsKey),
			Endpoint:  sqsEndpoint,
			Region:    sqsRegion,
		},
	}

	sess, err := session.NewSession(
		&aws.Config{
			Endpoint:         aws.String(sqsEndpoint),
			Region:           aws.String(sqsRegion),
			S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
			Credentials: credentials.NewStaticCredentials(
				sqsUser,
				sqsQueueName,
				"",
			),
		},
	)
	require.NoError(t, err)

	sqsClient := sqs.New(sess)
	queueURL, err := getQueueURL(sqsClient, sqsQueueName)
	require.NoError(t, err)

	sendAllMessages(t, 1240, testCasePath, queueURL, sqsClient)

	time.Sleep(5 * time.Second)
	cp := testutil.NewFakeClientWithTransferState()

	sourceOne, err := source.NewSource(src, "test-1", logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cp)
	require.NoError(t, err)
	sourceTwo, err := source.NewSource(src, "test-2", logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cp)
	require.NoError(t, err)

	sink1 := mockAsyncSink{}
	sink2 := mockAsyncSink{}

	go func() {
		err := sourceOne.Run(&sink1)
		logger.Log.Errorf("probably context canceled error in the middle of SQS request due to sourceOne.Stop() being called %v", err)
	}()

	time.Sleep(5 * time.Second) // so that we have some disparity in the data read by the two sources

	go func() {
		err := sourceTwo.Run(&sink2)
		logger.Log.Errorf("probably context canceled error in the middle of SQS request due to sourceTwo.Stop() being called %v", err)
	}()

	for {
		if sink1.getCurrentlyRead()+sink2.getCurrentlyRead() == 426560 {
			// should be done draining SQS of messages, stop sources
			sourceOne.Stop()
			sourceTwo.Stop()

			// ensure both sources read messages and processed items
			require.NotZero(t, sink1.getCurrentlyRead())
			require.NotZero(t, sink2.getCurrentlyRead())

			logger.Log.Infof("SourceOne read: %v, SourceTwo read: %v", sink1.getCurrentlyRead(), sink2.getCurrentlyRead())
			break
		}

		time.Sleep(1 * time.Second)
	}

	// check that no more messages are left in queue
	checkNoMoreMessagesLeft(t, sqsClient, queueURL)
}

func checkNoMoreMessagesLeft(t *testing.T, client *sqs.SQS, queueURL *string) {
	messages, err := client.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: aws.Int64(10), // maximum is 10, but fewer  msg can be delivered
		WaitTimeSeconds:     aws.Int64(20), // reduce cost by switching to long polling, 20s is max wait time
		VisibilityTimeout:   aws.Int64(21), // set read timeout to 21 s
	})
	require.NoError(t, err)
	require.Zero(t, len(messages.Messages))
}

func getQueueURL(sqsClient *sqs.SQS, queueName string) (*string, error) {
	res, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		return nil, err
	} else {
		return res.QueueUrl, nil
	}
}

func sendAllMessages(t *testing.T, amount int, path string, queueURL *string, sqsClient *sqs.SQS) {
	for i := 0; i < amount; i++ {
		body := fmt.Sprintf(messageBody, path, fmt.Sprintf("data%v.csv", i))
		err := sendMessageToQueue(&body, queueURL, sqsClient)
		require.NoError(t, err)
	}
}

func sendMessageToQueue(body, queueURL *string, sqsClient *sqs.SQS) error {
	_, err := sqsClient.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: body,
	})

	return err
}
