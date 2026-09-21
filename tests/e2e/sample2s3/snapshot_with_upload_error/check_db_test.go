package snapshotwithuploaderror

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	provider_sample "github.com/transferia/transferia/pkg/providers/sample"
	"github.com/transferia/transferia/tests/helpers/delivery"
	http_proxy "github.com/transferia/transferia/tests/helpers/proxies/http_proxy"
	_ "github.com/transferia/transferia/tests/helpers/registration/s3"
	_ "github.com/transferia/transferia/tests/helpers/registration/sample"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	testBucket    = envOrDefault("TEST_BUCKET", "barrel")
	testAccessKey = envOrDefault("TEST_ACCESS_KEY_ID", "1234567890")
	testSecret    = envOrDefault("TEST_SECRET_ACCESS_KEY", "abcdefabcdef")
)

func envOrDefault(key string, def string) string {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}
	return def
}

func createBucket(t *testing.T, cfg *s3_model.S3Destination) {
	t.Helper()
	sess, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		Credentials: aws_credentials.NewStaticCredentials(
			cfg.AccessKey, cfg.Secret, "",
		),
	})
	require.NoError(t, err)
	logger.Log.Info("create bucket", log.Any("bucket", cfg.Bucket))
	res, err := aws_s3.New(sess).CreateBucket(&aws_s3.CreateBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	require.NoError(t, err)
	logger.Log.Info("create bucket result", log.Any("res", res))
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func startS3UploadPartDenyProxy(t *testing.T, failOnNthUploadPart int) (endpoint string, cleanup func()) {
	t.Helper()

	s3Port := os.Getenv("S3MDS_PORT")
	if s3Port == "" {
		t.Skip("S3MDS_PORT is not set (need s3mds recipe)")
	}

	targetAddr := "127.0.0.1:" + s3Port
	proxy := http_proxy.NewHTTPProxyWithPortAllocation(targetAddr)

	var mu sync.Mutex
	uploadPartCount := make(map[string]int)
	proxy.ResponsePolicy = func(req *http.Request, _ []byte) (bool, int, http.Header, []byte) {
		if failOnNthUploadPart <= 0 || req.Method != http.MethodPut {
			return false, 0, nil, nil
		}

		q := req.URL.Query()
		uploadID := q.Get("uploadId")
		partNumber := q.Get("partNumber")
		if uploadID == "" || partNumber == "" {
			return false, 0, nil, nil
		}

		mu.Lock()
		uploadPartCount[uploadID]++
		n := uploadPartCount[uploadID]
		mu.Unlock()
		if n < failOnNthUploadPart {
			return false, 0, nil, nil
		}

		return true, http.StatusForbidden, http.Header{"Content-Type": []string{"application/xml"}}, []byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>AccessDenied</Code>
  <Message>Access Denied</Message>
  <RequestId>fake-request-id</RequestId>
  <HostId></HostId>
</Error>
`)
	}

	worker := proxy.RunAsync()
	return fmt.Sprintf("http://127.0.0.1:%d", proxy.ListenPort), func() {
		require.NoError(t, worker.Close())
	}
}

func TestSnapshotMultipartAccessDenied(t *testing.T) {
	src := provider_sample.RecipeSource()
	src.SnapshotEventCount = 2000000
	src.MaxSampleData = 100000

	dst := &s3_model.S3Destination{
		OutputFormat:     model.ParsingFormatJSON,
		BufferSize:       1 * 1024 * 1024,
		BufferInterval:   time.Second * 5,
		Bucket:           testBucket,
		AccessKey:        testAccessKey,
		S3ForcePathStyle: true,
		Secret:           testSecret,
		Layout:           "test",
		Region:           "eu-central1",
		PartSize:         5 * 1024 * 1024,
		Concurrency:      1,
	}
	dst.WithDefaults()

	ep, cleanup := startS3UploadPartDenyProxy(t, 1)
	defer cleanup()
	dst.Endpoint = ep
	createBucket(t, dst)

	transferhelpers.InitSrcDst(transferhelpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)

	t.Logf("sample snapshot: SnapshotEventCount=%d PartSize=%d proxy=%s FailOnNthUploadPart=1",
		src.SnapshotEventCount, dst.PartSize, ep)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
	w, actErr := delivery.ActivateErr(transfer)
	t.Logf("ActivateErr: %v", actErr)
	require.Error(t, actErr)
	require.Nil(t, w)

	msg := actErr.Error()
	require.Contains(t, msg, "MultipartUpload")
	require.Contains(t, msg, "upload multipart failed")
	require.Contains(t, msg, "AccessDenied")
	require.Contains(t, msg, "upload id:")
}
