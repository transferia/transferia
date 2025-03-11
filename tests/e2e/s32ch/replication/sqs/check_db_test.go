package sqs

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	dp_model "github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
	"github.com/transferria/transferria/pkg/providers/s3"
	"github.com/transferria/transferria/tests/helpers"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

var (
	dst = model.ChDestination{
		ShardsList: []model.ClickHouseShard{
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
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             dp_model.Drop,
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
	src := s3.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath

	// for schema deduction
	s3.UploadOne(t, src, "test_unescaped_files/simple=1234.jsonl")
	time.Sleep(time.Second)

	src.TableNamespace = "test"
	src.TableName = "unescaped"
	src.InputFormat = dp_model.ParsingFormatJSONLine
	src.EventSource.SQS = &s3.SQS{
		QueueName: sqsQueueName,
		ConnectionConfig: s3.ConnectionConfig{
			AccessKey: sqsUser,
			SecretKey: dp_model.SecretString(sqsKey),
			Endpoint:  sqsEndpoint,
			Region:    sqsRegion,
		},
	}
	src.WithDefaults()
	dst.WithDefaults()
	src.Format.JSONLSetting.BlockSize = 1 * 1024 * 1024

	transfer := helpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeIncrementOnly)
	helpers.Activate(t, transfer)

	if os.Getenv("S3MDS_PORT") != "" {
		src.Bucket = "data6"
		s3.CreateBucket(t, src)
		s3.PrepareTestCase(t, src, src.PathPrefix)
	}

	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(sqsEndpoint),
		Region:           aws.String(sqsRegion),
		S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			sqsUser, string(sqsQueueName), "",
		),
	})
	require.NoError(t, err)

	sqsClient := sqs.New(sess)
	queueURL, err := getQueueURL(sqsClient, sqsQueueName)
	require.NoError(t, err)

	err = sendMessageToQueue(aws.String(fmt.Sprintf(messageBody, testCasePath, "simple%3D1234.jsonl")), queueURL, sqsClient)
	require.NoError(t, err)

	err = helpers.WaitDestinationEqualRowsCount("test", "unescaped", helpers.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 3)
	require.NoError(t, err)

	err = sendMessageToQueue(aws.String(fmt.Sprintf(messageBody, testCasePath, "simple%3D1234+%281%29.jsonl")), queueURL, sqsClient)
	require.NoError(t, err)

	err = helpers.WaitDestinationEqualRowsCount("test", "unescaped", helpers.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 6)
	require.NoError(t, err)

	err = sendMessageToQueue(aws.String(fmt.Sprintf(messageBody, testCasePath, "simple%3D1234+%28copy%29.jsonl")), queueURL, sqsClient)
	require.NoError(t, err)

	err = helpers.WaitDestinationEqualRowsCount("test", "unescaped", helpers.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 9)
	require.NoError(t, err)

	err = sendMessageToQueue(aws.String(fmt.Sprintf(messageBody, testCasePath, "simple%3D+test++wtih+spaces.jsonl")), queueURL, sqsClient)
	require.NoError(t, err)

	err = helpers.WaitDestinationEqualRowsCount("test", "unescaped", helpers.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 12)
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
