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
	testCasePath := "thousands_of_csv_files"
	src := s3.PrepareCfg(t, "data7", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, src, src.PathPrefix)
	}

	time.Sleep(5 * time.Second)

	src.TableNamespace = "test"
	src.TableName = "data"
	src.InputFormat = dp_model.ParsingFormatCSV
	src.WithDefaults()
	dst.WithDefaults()
	src.Format.CSVSetting.BlockSize = 10000000
	src.ReadBatchSize = 4000 // just for testing so its faster, normally much smaller
	src.Format.CSVSetting.QuoteChar = "\""

	src.EventSource.SQS = &s3.SQS{
		QueueName: sqsQueueName,
		ConnectionConfig: s3.ConnectionConfig{
			AccessKey: sqsUser,
			SecretKey: dp_model.SecretString(sqsKey),
			Endpoint:  sqsEndpoint,
			Region:    sqsRegion,
		},
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

	sendAllMessages(t, 1240, testCasePath, queueURL, sqsClient)

	time.Sleep(5 * time.Second)

	start := time.Now()
	transfer := helpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeIncrementOnly)
	helpers.Activate(t, transfer)

	err = helpers.WaitDestinationEqualRowsCount("test", "data", helpers.GetSampleableStorageByModel(t, transfer.Dst), 500*time.Second, 426560)
	require.NoError(t, err)
	finish := time.Now()
	duration := finish.Sub(start)
	fmt.Println("Execution took:", duration)
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
