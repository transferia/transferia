package delete

import (
	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

var MatchAllKeys = func(string) bool { return true }

func DeleteMatchingObjects(
	s3Client deleteS3Client,
	bucket string,
	listPrefix string,
	matchKey func(string) bool,
) (int, error) {
	batch := NewS3DeleteBatch(s3Client, bucket)

	var continuationToken *string
	for {
		out, err := s3Client.ListObjectsV2(&aws_s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(listPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return 0, xerrors.Errorf("unable to list objects for drop under prefix %q: %w", listPrefix, err)
		}
		for _, obj := range out.Contents {
			key := aws.StringValue(obj.Key)
			if !matchKey(key) {
				continue
			}
			if err := batch.Add(key); err != nil {
				return 0, xerrors.Errorf("unable to flush delete batch: %w", err)
			}
		}
		if !aws.BoolValue(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}
	if err := batch.Flush(); err != nil {
		return 0, xerrors.Errorf("unable to flush final delete batch: %w", err)
	}

	return batch.Deleted(), nil
}
