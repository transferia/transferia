package s3util

import (
	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/glob"
	"go.ytsaurus.tech/library/go/core/log"
)

// SkipObject returns true if an object should be skipped.
// An object is skipped if the file type does not match the one covered by the reader or
// if the objects name/path is not included in the path pattern or if custom filter returned false.
func SkipObject(file *aws_s3.Object, pathPattern, splitter string, filter func(*aws_s3.Object) bool) bool {
	if file == nil {
		return true
	}
	keepObject := filter(file) && glob.SplitMatch(pathPattern, *file.Key, splitter)
	return !keepObject
}

// ListFilesWithCallback lists objects under prefix and invokes fn for each object matching pathPattern and filter.
// If fn returns stop=true, listing stops immediately (no further S3 pages are requested when the current page is exhausted).
// If fn returns a non-nil error, listing aborts and the error is returned.
func ListFilesWithCallback(
	bucket, pathPrefix, pathPattern string,
	client s3iface.S3API,
	logger log.Logger,
	filter func(*aws_s3.Object) bool,
	fn func(*aws_s3.Object) (stop bool, err error),
) error {
	var currentMarker *string
	for {
		listBatchSize := int64(1000)
		files, err := client.ListObjects(&aws_s3.ListObjectsInput{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(pathPrefix),
			MaxKeys: aws.Int64(listBatchSize),
			Marker:  currentMarker,
		})
		if err != nil {
			return xerrors.Errorf("unable to load file list: %w", err)
		}

		for _, file := range files.Contents {
			if SkipObject(file, pathPattern, "|", filter) {
				logger.Debugf("ListFilesWithCallback - file did not pass type/path check, skipping: file %s, pathPattern: %s", *file.Key, pathPattern)
				continue
			}
			stop, err := fn(file)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		if len(files.Contents) > 0 {
			currentMarker = files.Contents[len(files.Contents)-1].Key
		}

		if int64(len(files.Contents)) < listBatchSize {
			break
		}
	}

	return nil
}

// ListFiles lists all files matching the pathPattern in a bucket.
// A fast circuit breaker is built in for schema resolution where we do not need the full list of objects.
func ListFiles(bucket, pathPrefix, pathPattern string, client s3iface.S3API, logger log.Logger, limit *int, filter func(*aws_s3.Object) bool) ([]*aws_s3.Object, error) {
	var res []*aws_s3.Object
	err := ListFilesWithCallback(bucket, pathPrefix, pathPattern, client, logger, filter, func(file *aws_s3.Object) (bool, error) {
		res = append(res, file)
		if limit != nil && len(res) >= *limit {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// FileSize returns file's size if it stored in file.Size, otherwise it gets size by S3 API call.
// NOTE: FileSize only returns file's size and do NOT changes original file.Size field.
func FileSize(bucket string, file *aws_s3.Object, client s3iface.S3API, logger log.Logger) (uint64, error) {
	if file == nil {
		return 0, xerrors.New("provided file is nil")
	}
	if file.Key == nil {
		return 0, xerrors.New("provided file key is nil")
	}
	if file.Size != nil {
		if *file.Size < 0 {
			return 0, xerrors.Errorf("size of file %s is negative (%d)", *file.Key, *file.Size)
		}
		return uint64(*file.Size), nil
	}
	logger.Debugf("Size of file %s is unknown, measuring it", *file.Key)
	resp, err := client.GetObjectAttributes(&aws_s3.GetObjectAttributesInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(*file.Key),
		ObjectAttributes: aws.StringSlice([]string{aws_s3.ObjectAttributesObjectSize}),
	})
	if err != nil {
		return 0, xerrors.Errorf("unable to get file %s size attribute: %w", *file.Key, err)
	}
	if resp.ObjectSize == nil {
		return 0, xerrors.Errorf("returned by s3-api size of file %s is nil", *file.Key)
	}
	if *resp.ObjectSize < 0 {
		return 0, xerrors.Errorf("measured size of file %s is negative (%d)", *file.Key, *resp.ObjectSize)
	}
	return uint64(*resp.ObjectSize), nil
}
