package sink

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	s3_delete "github.com/transferia/transferia/pkg/providers/s3/s3util/delete"
	s3_v1_sink_client "github.com/transferia/transferia/pkg/providers/s3/v1/sink/client"
	"go.ytsaurus.tech/library/go/core/log"
)

func replaceUploads(
	logger log.Logger,
	s3Client s3_v1_sink_client.S3Client,
	bucket string,
	prefix string,
	operationTimestamp string,
) error {
	uploads, err := listPendingUploads(s3Client, bucket, prefix, operationTimestamp)
	if err != nil {
		return xerrors.Errorf("list pending uploads: %w", err)
	}
	if len(uploads) == 0 {
		return nil
	}

	// Delete old files at each real table prefix before making new files visible.
	for _, prefix := range uniqueUploadPrefixes(uploads) {
		n, err := s3_delete.DeleteMatchingObjects(s3Client, bucket, prefix, s3_delete.MatchAllKeys)
		if err != nil {
			return xerrors.Errorf("delete prefix %s: %w", prefix, err)
		}
		logger.Info("deleted old objects for replace", log.Int("count", n), log.String("prefix", prefix))
	}

	// Complete all pending uploads, making new files visible.
	for _, upload := range uploads {
		parts, err := listAllParts(
			s3Client,
			bucket,
			aws.StringValue(upload.Key),
			aws.StringValue(upload.UploadId),
		)
		if err != nil {
			return xerrors.Errorf("list parts for %s: %w", aws.StringValue(upload.Key), err)
		}
		if _, err := s3Client.CompleteMultipartUpload(&aws_s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(bucket),
			Key:             upload.Key,
			UploadId:        upload.UploadId,
			MultipartUpload: &aws_s3.CompletedMultipartUpload{Parts: parts},
		}); err != nil {
			return xerrors.Errorf("complete upload %s: %w", aws.StringValue(upload.Key), err)
		}
	}
	return nil
}

// uniqueUploadPrefixes extracts the unique directory prefix (path up to last "/")
// from each multipart upload key.
func uniqueUploadPrefixes(uploads []*aws_s3.MultipartUpload) []string {
	seen := make(map[string]struct{})
	var prefixes []string
	for _, u := range uploads {
		key := aws.StringValue(u.Key)
		idx := strings.LastIndex(key, "/")
		if idx < 0 {
			continue
		}
		prefix := key[:idx+1]
		if _, ok := seen[prefix]; !ok {
			seen[prefix] = struct{}{}
			prefixes = append(prefixes, prefix)
		}
	}
	return prefixes
}

// listAllParts paginates ListParts for one multipart upload and returns
// the completed parts list suitable for CompleteMultipartUpload.
func listAllParts(s3Client s3_v1_sink_client.S3Client, bucket string, key string, uploadId string) ([]*aws_s3.CompletedPart, error) {
	var parts []*aws_s3.CompletedPart
	input := &aws_s3.ListPartsInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	}
	for {
		out, err := s3Client.ListParts(input)
		if err != nil {
			return nil, xerrors.Errorf("ListParts: %w", err)
		}
		for _, p := range out.Parts {
			parts = append(parts, &aws_s3.CompletedPart{
				ETag:       p.ETag,
				PartNumber: p.PartNumber,
			})
		}
		if !aws.BoolValue(out.IsTruncated) {
			break
		}
		input.PartNumberMarker = out.NextPartNumberMarker
	}
	return parts, nil
}

func listPendingUploads(
	s3Client s3_v1_sink_client.S3Client,
	bucket string,
	prefix string,
	operationTimestamp string,
) ([]*aws_s3.MultipartUpload, error) {
	var uploads []*aws_s3.MultipartUpload
	input := &aws_s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	for {
		out, err := s3Client.ListMultipartUploads(input)
		if err != nil {
			return nil, xerrors.Errorf("ListMultipartUploads: %w", err)
		}
		for _, u := range out.Uploads {
			if strings.Contains(aws.StringValue(u.Key), operationTimestamp) {
				uploads = append(uploads, u)
			}
		}
		if !aws.BoolValue(out.IsTruncated) {
			break
		}
		input.KeyMarker = out.NextKeyMarker
		input.UploadIdMarker = out.NextUploadIdMarker
	}
	return uploads, nil
}
