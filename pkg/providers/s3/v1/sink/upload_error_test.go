package sink

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3/encryption"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	"github.com/transferia/transferia/pkg/providers/s3/v1/sink/testutil"
	"github.com/transferia/transferia/pkg/stats"
)

// failingUploadClient reads the whole body like a real uploader and then fails, as CompleteMultipartUpload can.
type failingUploadClient struct {
	*testutil.MockS3Client
}

func (c *failingUploadClient) Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error) {
	if _, err := io.Copy(io.Discard, input.Body); err != nil {
		return nil, xerrors.Errorf("unable to read body: %w", err)
	}
	return nil, xerrors.New("CompleteMultipartUpload failed")
}

func TestSnapshotSinkReportsUploadErrorAfterBodyIsRead(t *testing.T) {
	key, err := encryption.ParseKey(model.SecretString(strings.Repeat("0f", encryption.KeySize)))
	require.NoError(t, err)

	for name, encryptionKey := range map[string]*encryption.Key{"plain": nil, "encrypted": key} {
		t.Run(name, func(t *testing.T) {
			cfg := &s3_v1_model.S3Destination{
				Bucket:         "bucket",
				SerializerType: model.ParsingFormatJSON,
				Serializer:     s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},
			}
			currSink := &SnapshotSink{
				s3Client:           &failingUploadClient{MockS3Client: testutil.NewMockS3Client()},
				cfg:                cfg,
				operationTimestamp: "1700000000",
				snapshotWriter:     nil,
				logger:             logger.Log,
				metrics:            stats.NewSinkerStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
				fileSplitter:       newFileSplitter(0, 0),
				serializer:         cfg.GetSerializer(),
				encryptionKey:      encryptionKey,
			}

			require.NoError(t, currSink.Push([]abstract.ChangeItem{
				{Kind: abstract.InitTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
			}))
			require.NoError(t, currSink.Push(generateBullets("test_table", 10)))
			err := currSink.Push([]abstract.ChangeItem{
				{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
			})
			require.ErrorContains(t, err, "CompleteMultipartUpload failed")
		})
	}
}
