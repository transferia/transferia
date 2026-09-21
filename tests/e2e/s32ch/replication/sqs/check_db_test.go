package sqs

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

var (
	dst = clickhouse_model.ChDestination{
		ShardsList: []clickhouse_model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:                "default",
		Password:            "",
		Database:            "test",
		HTTPPort:            testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             model.Drop,
	}
	sqsEndpoint  = fmt.Sprintf("http://localhost:%s", os.Getenv("SQS_PORT"))
	sqsUser      = "test_s3_replication_sqs_user"
	sqsKey       = "unused"
	sqsQueueName = "test_s3_replication_sqs_queue"
	sqsRegion    = "yandex"
	messageBody  = `{"Records":[{"eventTime":"2023-08-09T11:46:36.337Z","eventName":"ObjectCreated:Put","s3":{"configurationId":"NewObjectCreateEvent","bucket":{"name":"test_csv_replication"},"object":{"key":"%s/%s","size":627}}}]}`
)

func TestNativeS3PathsAreUnescaped(t *testing.T) {
	testCasePath := "test_unescaped_files"
	src := s3recipe.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath

	// for schema deduction
	s3recipe.UploadOne(t, src, "test_unescaped_files/simple=1234.jsonl")
	time.Sleep(time.Second)

	src.TableNamespace = "test"
	src.TableName = "unescaped"
	src.InputFormat = model.ParsingFormatJSONLine
	src.EventSource.SQS = &s3_model.SQS{
		QueueName: sqsQueueName,
		ConnectionConfig: s3_model.ConnectionConfig{
			AccessKey: sqsUser,
			SecretKey: model.SecretString(sqsKey),
			Endpoint:  sqsEndpoint,
			Region:    sqsRegion,
		},
	}
	src.WithDefaults()
	dst.WithDefaults()
	src.Format.JSONLSetting.BlockSize = 1 * 1024 * 1024

	transfer := transferhelpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeIncrementOnly)
	delivery.Activate(t, transfer)

	if os.Getenv("S3MDS_PORT") != "" {
		src.Bucket = "data6"
		s3recipe.CreateBucket(t, src)
		s3recipe.PrepareTestCase(t, src, src.PathPrefix)
	}

	sess, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(sqsEndpoint),
		Region:           aws.String(sqsRegion),
		S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
		Credentials: aws_credentials.NewStaticCredentials(
			sqsUser, string(sqsQueueName), "",
		),
	})
	require.NoError(t, err)

	sqsClient := sqs.New(sess)
	queueURL, err := getQueueURL(sqsClient, sqsQueueName)
	require.NoError(t, err)

	err = sendMessageToQueue(aws.String(fmt.Sprintf(messageBody, testCasePath, "simple%3D1234.jsonl")), queueURL, sqsClient)
	require.NoError(t, err)

	err = storage.WaitDestinationEqualRowsCount("test", "unescaped", storagecomparison.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 3)
	require.NoError(t, err)

	err = sendMessageToQueue(aws.String(fmt.Sprintf(messageBody, testCasePath, "simple%3D1234+%281%29.jsonl")), queueURL, sqsClient)
	require.NoError(t, err)

	err = storage.WaitDestinationEqualRowsCount("test", "unescaped", storagecomparison.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 6)
	require.NoError(t, err)

	err = sendMessageToQueue(aws.String(fmt.Sprintf(messageBody, testCasePath, "simple%3D1234+%28copy%29.jsonl")), queueURL, sqsClient)
	require.NoError(t, err)

	err = storage.WaitDestinationEqualRowsCount("test", "unescaped", storagecomparison.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 9)
	require.NoError(t, err)

	err = sendMessageToQueue(aws.String(fmt.Sprintf(messageBody, testCasePath, "simple%3D+test++wtih+spaces.jsonl")), queueURL, sqsClient)
	require.NoError(t, err)

	err = storage.WaitDestinationEqualRowsCount("test", "unescaped", storagecomparison.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 12)
	require.NoError(t, err)
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

func sendMessageToQueue(body, queueURL *string, sqsClient *sqs.SQS) error {
	_, err := sqsClient.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: body,
	})

	return err
}
