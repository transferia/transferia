package sink

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	"github.com/transferia/transferia/pkg/providers/s3/v1/sink/testutil"
	"github.com/transferia/transferia/pkg/stats"
)

// TestReplaceHappyPath verifies that Replace() deletes old files per table
// prefix and completes all pending multipart uploads matching the sink's
// operationTimestamp, ignoring uploads from other operations.
func TestReplaceHappyPath(t *testing.T) {
	const bucket = "test-bucket"
	const opTS = "1700000000"

	mock := testutil.NewMockS3Client()
	mock.BucketFiles[bucket] = map[string][]byte{
		"pfx/ns/tbl/old-file.json": []byte("old"),
	}
	mock.PendingUploads = []*aws_s3.MultipartUpload{
		{Key: aws.String("pfx/ns/tbl/part-" + opTS + "-abc.00000.json"), UploadId: aws.String("uid1")},
		{Key: aws.String("pfx/ns/tbl/part-" + opTS + "-abc.00001.json"), UploadId: aws.String("uid2")},
		// upload from a different operation — must be ignored
		{Key: aws.String("pfx/ns/tbl/part-9999999999-xyz.00000.json"), UploadId: aws.String("uid3")},
	}
	mock.UploadParts["uid1"] = []*aws_s3.Part{{ETag: aws.String("e1"), PartNumber: aws.Int64(1)}}
	mock.UploadParts["uid2"] = []*aws_s3.Part{{ETag: aws.String("e2"), PartNumber: aws.Int64(1)}}

	sink := newReplaceSink(t, bucket, "pfx", opTS, mock)
	require.NoError(t, sink.Replace())

	require.Len(t, mock.CompletedUploads, 2)
	require.Empty(t, mock.AbortedUploads)
	// old file must be gone
	require.Empty(t, mock.BucketFiles[bucket])

	completedIDs := map[string]struct{}{}
	for _, in := range mock.CompletedUploads {
		require.Equal(t, bucket, aws.StringValue(in.Bucket))
		completedIDs[aws.StringValue(in.UploadId)] = struct{}{}
		require.Len(t, in.MultipartUpload.Parts, 1)
	}
	require.Contains(t, completedIDs, "uid1")
	require.Contains(t, completedIDs, "uid2")
	require.NotContains(t, completedIDs, "uid3")
}

// TestReplaceDeleteError verifies that when prefix deletion fails, Replace()
// returns the error without completing uploads. Abort of pending uploads should be
// CleanupDestination's job, not Replace's.
func TestReplaceDeleteError(t *testing.T) {
	const bucket = "test-bucket"
	const opTS = "1700000000"

	mock := testutil.NewMockS3Client()
	mock.BucketFiles[bucket] = map[string][]byte{
		"pfx/ns/tbl/old.json": []byte("x"),
	}
	mock.DeleteObjectsErr = errDeleteFailed
	mock.PendingUploads = []*aws_s3.MultipartUpload{
		{Key: aws.String("pfx/ns/tbl/part-" + opTS + "-abc.00000.json"), UploadId: aws.String("uid1")},
	}
	mock.UploadParts["uid1"] = []*aws_s3.Part{{ETag: aws.String("e1"), PartNumber: aws.Int64(1)}}

	sink := newReplaceSink(t, bucket, "pfx", opTS, mock)
	err := sink.Replace()
	require.Error(t, err)
	require.ErrorContains(t, err, "delete prefix")
	require.Empty(t, mock.AbortedUploads)
	require.Empty(t, mock.CompletedUploads)
}

func TestReplaceNoPendingUploads(t *testing.T) {
	const bucket = "test-bucket"

	mock := testutil.NewMockS3Client()
	mock.BucketFiles[bucket] = map[string][]byte{
		"pfx/ns/tbl/old.json": []byte("keep"),
	}

	sink := newReplaceSink(t, bucket, "pfx", "1700000000", mock)
	require.NoError(t, sink.Replace())

	require.Empty(t, mock.CompletedUploads)
	require.Empty(t, mock.DeleteObjectsCalls)
	require.Equal(t, []byte("keep"), mock.BucketFiles[bucket]["pfx/ns/tbl/old.json"])
}

func TestReplaceMultipleTablePrefixes(t *testing.T) {
	const bucket = "test-bucket"
	const opTS = "1700000000"

	mock := testutil.NewMockS3Client()
	mock.BucketFiles[bucket] = map[string][]byte{
		"pfx/ns/tbl_a/old.json":  []byte("a"),
		"pfx/ns/tbl_b/old.json":  []byte("b"),
		"pfx/ns/tbl_c/keep.json": []byte("c"), // no matching upload — must stay
	}
	mock.PendingUploads = []*aws_s3.MultipartUpload{
		{Key: aws.String("pfx/ns/tbl_a/part-" + opTS + "-a.00000.json"), UploadId: aws.String("uid-a")},
		{Key: aws.String("pfx/ns/tbl_b/part-" + opTS + "-b.00000.json"), UploadId: aws.String("uid-b")},
	}
	mock.UploadParts["uid-a"] = []*aws_s3.Part{{ETag: aws.String("ea"), PartNumber: aws.Int64(1)}}
	mock.UploadParts["uid-b"] = []*aws_s3.Part{{ETag: aws.String("eb"), PartNumber: aws.Int64(1)}}

	require.NoError(t, replaceUploads(logger.Log, mock, bucket, "pfx", opTS))

	require.Len(t, mock.CompletedUploads, 2)
	require.NotContains(t, mock.BucketFiles[bucket], "pfx/ns/tbl_a/old.json")
	require.NotContains(t, mock.BucketFiles[bucket], "pfx/ns/tbl_b/old.json")
	require.Equal(t, []byte("c"), mock.BucketFiles[bucket]["pfx/ns/tbl_c/keep.json"])
}

func TestReplaceListUploadsError(t *testing.T) {
	mock := testutil.NewMockS3Client()
	mock.ListMultipartUploadsErr = xerrors.New("list uploads failed")

	err := replaceUploads(logger.Log, mock, "test-bucket", "pfx", "1700000000")
	require.Error(t, err)
	require.ErrorContains(t, err, "list pending uploads")
	require.Empty(t, mock.CompletedUploads)
}

func TestReplaceListPartsError(t *testing.T) {
	const bucket = "test-bucket"
	const opTS = "1700000000"

	mock := testutil.NewMockS3Client()
	mock.PendingUploads = []*aws_s3.MultipartUpload{
		{Key: aws.String("pfx/ns/tbl/part-" + opTS + "-abc.00000.json"), UploadId: aws.String("uid1")},
	}
	mock.ListPartsErr = xerrors.New("list parts failed")

	err := replaceUploads(logger.Log, mock, bucket, "pfx", opTS)
	require.Error(t, err)
	require.ErrorContains(t, err, "list parts")
	require.Empty(t, mock.CompletedUploads)
}

func TestReplaceCompleteError(t *testing.T) {
	const bucket = "test-bucket"
	const opTS = "1700000000"

	mock := testutil.NewMockS3Client()
	mock.BucketFiles[bucket] = map[string][]byte{
		"pfx/ns/tbl/old.json": []byte("x"),
	}
	mock.PendingUploads = []*aws_s3.MultipartUpload{
		{Key: aws.String("pfx/ns/tbl/part-" + opTS + "-abc.00000.json"), UploadId: aws.String("uid1")},
	}
	mock.UploadParts["uid1"] = []*aws_s3.Part{{ETag: aws.String("e1"), PartNumber: aws.Int64(1)}}
	mock.CompleteMultipartUploadErr = xerrors.New("complete failed")

	err := replaceUploads(logger.Log, mock, bucket, "pfx", opTS)
	require.Error(t, err)
	require.ErrorContains(t, err, "complete upload")
	// old objects already deleted before complete
	require.Empty(t, mock.BucketFiles[bucket])
	require.Empty(t, mock.CompletedUploads)
}

func TestUniqueUploadPrefixes(t *testing.T) {
	uploads := []*aws_s3.MultipartUpload{
		{Key: aws.String("pfx/ns/tbl_a/file1.json")},
		{Key: aws.String("pfx/ns/tbl_a/file2.json")},
		{Key: aws.String("pfx/ns/tbl_b/file1.json")},
		{Key: aws.String("no-slash.json")}, // skipped — no directory prefix
	}
	require.Equal(t, []string{"pfx/ns/tbl_a/", "pfx/ns/tbl_b/"}, uniqueUploadPrefixes(uploads))
	require.Empty(t, uniqueUploadPrefixes(nil))
}

func TestListPendingUploadsPaginationAndFilter(t *testing.T) {
	const opTS = "1700000000"

	mock := testutil.NewMockS3Client()
	mock.ListMultipartUploadsPageSize = 1
	mock.PendingUploads = []*aws_s3.MultipartUpload{
		{Key: aws.String("pfx/a-" + opTS + ".json"), UploadId: aws.String("uid1")},
		{Key: aws.String("pfx/b-other.json"), UploadId: aws.String("uid2")},
		{Key: aws.String("pfx/c-" + opTS + ".json"), UploadId: aws.String("uid3")},
		{Key: aws.String("other/d-" + opTS + ".json"), UploadId: aws.String("uid4")}, // outside prefix
	}

	uploads, err := listPendingUploads(mock, "test-bucket", "pfx", opTS)
	require.NoError(t, err)
	require.Len(t, uploads, 2)
	require.Equal(t, "uid1", aws.StringValue(uploads[0].UploadId))
	require.Equal(t, "uid3", aws.StringValue(uploads[1].UploadId))
}

func TestListAllPartsPagination(t *testing.T) {
	mock := testutil.NewMockS3Client()
	mock.ListPartsPageSize = 2
	mock.UploadParts["uid1"] = []*aws_s3.Part{
		{ETag: aws.String("e1"), PartNumber: aws.Int64(1)},
		{ETag: aws.String("e2"), PartNumber: aws.Int64(2)},
		{ETag: aws.String("e3"), PartNumber: aws.Int64(3)},
	}

	parts, err := listAllParts(mock, "test-bucket", "pfx/key.json", "uid1")
	require.NoError(t, err)
	require.Len(t, parts, 3)
	require.Equal(t, int64(1), aws.Int64Value(parts[0].PartNumber))
	require.Equal(t, "e1", aws.StringValue(parts[0].ETag))
	require.Equal(t, int64(2), aws.Int64Value(parts[1].PartNumber))
	require.Equal(t, int64(3), aws.Int64Value(parts[2].PartNumber))
	require.Equal(t, "e3", aws.StringValue(parts[2].ETag))
}

var errDeleteFailed = xerrors.New("delete failed")

func newReplaceSink(t *testing.T, bucket, prefix, opTS string, mock *testutil.MockS3Client) *SnapshotSink {
	t.Helper()
	cfg := &s3_v1_model.S3Destination{
		Bucket: bucket,
		Prefix: prefix,
	}
	return &SnapshotSink{
		cfg:                cfg,
		s3Client:           mock,
		logger:             logger.Log,
		metrics:            stats.NewSinkerStats(solomon.NewRegistry(nil)),
		operationTimestamp: opTS,
	}
}
