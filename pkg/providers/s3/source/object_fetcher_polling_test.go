package source

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/sink/testutil"
	"github.com/transferia/transferia/pkg/stats"
)

func TestGetFileList(t *testing.T) {
	t.Skip()

	src := s3.PrepareCfg(t, "data4", "")

	src.PathPrefix = "test_thousands_csv_files"
	if os.Getenv("S3MDS_PORT") != "" {
		// for local recipe we need to upload test case to internet
		src.PathPrefix = "test_thousands_csv_files"
		s3.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}

	src.InputFormat = model.ParsingFormatCSV
	src.WithDefaults()
	src.Format.CSVSetting.DoubleQuote = true
	src.Format.CSVSetting.QuoteChar = "\""
	src.Format.CSVSetting.NewlinesInValue = true

	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(src.ConnectionConfig.Endpoint),
		Region:           aws.String(src.ConnectionConfig.Region),
		S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			src.ConnectionConfig.AccessKey, string(src.ConnectionConfig.SecretKey), "",
		),
	})
	require.NoError(t, err)

	metrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	r, err := reader.New(src, logger.Log, sess, metrics)
	require.NoError(t, err)

	source := &pollingSource{
		src:    src,
		ctx:    context.Background(),
		logger: logger.Log,
		client: aws_s3.New(sess),
		reader: r,
		cp:     testutil.NewFakeClientWithTransferState(),
	}

	list, err := source.FetchObjects()
	require.NoError(t, err)
	require.Equal(t, 12435, len(list))

	listSecondRun, err := source.FetchObjects()
	require.NoError(t, err)

	require.Equal(t, 0, len(listSecondRun)) // nothing new on second fetch attempt
}

func TestTransferState(t *testing.T) {
	cp := testutil.NewFakeClientWithTransferState()

	test1 := Object{
		Name:         "test-1",
		LastModified: time.Date(2024, 3, 7, 15, 15, 16, 0, time.UTC),
	}
	test3 := Object{
		Name:         "test-3",
		LastModified: time.Date(2024, 3, 7, 15, 15, 16, 0, time.UTC),
	}

	source := &pollingSource{
		cp: cp,
		mu: sync.Mutex{},
		inflight: []inflightObject{
			{
				Object: test1,
				Done:   false,
			},
			{
				Object: Object{
					Name:         "test-2",
					LastModified: time.Date(2024, 3, 7, 15, 15, 15, 0, time.UTC),
				},
				Done: true,
			},
			{
				Object: test3,
				Done:   false,
			},
			{
				Object: Object{
					Name:         "test-4",
					LastModified: time.Date(2024, 3, 7, 15, 15, 17, 0, time.UTC),
				},
				Done: true,
			},
		},
	}

	err := source.Commit(test3)
	require.NoError(t, err)

	res, err := source.loadTransferState()
	require.NoError(t, err)
	require.Nil(t, res) // first object not done, still not persisting

	// lets now assume one new object comes in and the first object completes
	source.inflight = append(source.inflight, inflightObject{
		Object: Object{
			Name:         "test-5",
			LastModified: time.Date(2024, 0o3, 0o7, 15, 15, 18, 0, time.UTC),
		},
		Done: false,
	})

	err = source.Commit(test1)
	require.NoError(t, err)
	res, err = source.loadTransferState()

	require.NoError(t, err)
	require.NotNil(t, res) // 4/5 objects are done, the last modified object form the ones that are done is persisted

	// single file inflight
	source.inflight = []inflightObject{
		{
			Object: test1,
			Done:   false,
		},
	}
	err = source.Commit(test1)
	require.NoError(t, err)
	res, err = source.loadTransferState()
	require.NoError(t, err)
	require.NotNil(t, res)
}
