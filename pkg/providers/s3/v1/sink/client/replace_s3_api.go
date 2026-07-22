package client

import (
	"bytes"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
)

// replaceS3API adapts upload operations for Replace mode by leaving
// multipart uploads pending until Replace completes them.
type replaceS3API struct {
	s3iface.S3API
}

func (d *replaceS3API) CompleteMultipartUploadWithContext(
	ctx aws.Context,
	input *aws_s3.CompleteMultipartUploadInput,
	opts ...request.Option,
) (*aws_s3.CompleteMultipartUploadOutput, error) {
	return &aws_s3.CompleteMultipartUploadOutput{}, nil
}

// PutObjectRequest overrides the s3manager small-body path. s3manager calls
// PutObjectRequest (not PutObjectWithContext) when the upload body fits in one
// read. We clear the request's handler list and replace the Send handler with
// one that converts the call to a pending single-part multipart upload.
func (d *replaceS3API) PutObjectRequest(input *aws_s3.PutObjectInput) (*request.Request, *aws_s3.PutObjectOutput) {
	req, out := d.S3API.PutObjectRequest(input)

	// Clear all SDK handlers — we drive the request ourselves.
	req.Handlers = request.Handlers{}
	req.Handlers.Send.PushFront(func(r *request.Request) {
		putInput := r.Params.(*aws_s3.PutObjectInput)

		createOut, err := d.S3API.CreateMultipartUpload(&aws_s3.CreateMultipartUploadInput{
			Bucket:   putInput.Bucket,
			Key:      putInput.Key,
			Metadata: putInput.Metadata,
		})
		if err != nil {
			r.Error = xerrors.Errorf("create multipart upload: %w", err)
			return
		}
		uploadID := createOut.UploadId

		rollback := util.Rollbacks{}
		defer rollback.Do()
		rollback.Add(func() {
			_, _ = d.S3API.AbortMultipartUpload(&aws_s3.AbortMultipartUploadInput{
				Bucket:   putInput.Bucket,
				Key:      putInput.Key,
				UploadId: uploadID,
			})
		})

		data, err := io.ReadAll(putInput.Body)
		if err != nil {
			r.Error = xerrors.Errorf("read body: %w", err)
			return
		}

		if _, err := d.S3API.UploadPart(&aws_s3.UploadPartInput{
			Bucket:     putInput.Bucket,
			Key:        putInput.Key,
			UploadId:   uploadID,
			PartNumber: aws.Int64(1),
			Body:       bytes.NewReader(data),
		}); err != nil {
			r.Error = xerrors.Errorf("upload part: %w", err)
			return
		}

		rollback.Cancel()

		// Do NOT call CompleteMultipartUpload - Replace() will do it later.
		r.Data = out
	})

	return req, out
}
