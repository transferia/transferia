package s3raw

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/encryption"
	"github.com/transferia/transferia/pkg/stats"
)

// MemoryS3Client is a test helper that serves objects from memory, including range requests,
// so that readers built by NewS3RawReader (decompression, decryption, random access) work as with real S3.
type MemoryS3Client struct {
	s3iface.S3API
	Objects map[string][]byte
}

func (m *MemoryS3Client) HeadObjectWithContext(_ context.Context, input *aws_s3.HeadObjectInput, _ ...request.Option) (*aws_s3.HeadObjectOutput, error) {
	object, ok := m.Objects[*input.Key]
	if !ok {
		return nil, xerrors.Errorf("object %q not found", *input.Key)
	}
	return &aws_s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(object))),
		LastModified:  aws.Time(time.Now()),
	}, nil
}

func (m *MemoryS3Client) GetObjectWithContext(_ context.Context, input *aws_s3.GetObjectInput, _ ...request.Option) (*aws_s3.GetObjectOutput, error) {
	object, ok := m.Objects[*input.Key]
	if !ok {
		return nil, xerrors.Errorf("object %q not found", *input.Key)
	}
	if input.Range != nil {
		var start, end int
		if _, err := fmt.Sscanf(*input.Range, "bytes=%d-%d", &start, &end); err != nil {
			return nil, xerrors.Errorf("bad range %q: %w", *input.Range, err)
		}
		object = object[start:min(end+1, len(object))]
	}
	return &aws_s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(object)),
		ContentLength: aws.Int64(int64(len(object))),
	}, nil
}

var _ S3RawReaderBuilder = (*MemoryS3RawReaderBuilder)(nil)

// MemoryS3RawReaderBuilder builds real raw readers over MemoryS3Client, optionally decrypting objects.
type MemoryS3RawReaderBuilder struct {
	client        *MemoryS3Client
	encryptionKey *encryption.Key
}

func (b *MemoryS3RawReaderBuilder) BuildReader(ctx context.Context, _ s3iface.S3API, bucket string, key string, metrics *stats.SourceStats) (S3RawReader, error) {
	return NewS3RawReader(ctx, b.client, bucket, key, b.encryptionKey, metrics)
}

func (b *MemoryS3RawReaderBuilder) BuildClient(_ *aws_session.Session) s3iface.S3API {
	return b.client
}

func NewMemoryS3RawReaderBuilder(objects map[string][]byte, encryptionKey *encryption.Key) *MemoryS3RawReaderBuilder {
	return &MemoryS3RawReaderBuilder{
		client:        &MemoryS3Client{S3API: nil, Objects: objects},
		encryptionKey: encryptionKey,
	}
}
