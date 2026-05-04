package estimators

import (
	"context"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/format"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type ReaderCtorF = func(ctx context.Context, filePath string) (s3raw.S3RawReader, error)

func EstimateTotalSizeInBytes(ctx context.Context, lgr log.Logger, files []*aws_s3.Object, readerCtor ReaderCtorF) (uint64, s3raw.S3RawReader, error) {
	var sampleReader s3raw.S3RawReader
	multiplier := float64(1)
	sniffFiles := files
	if len(files) > s3_reader.EstimateFilesLimit {
		multiplier = float64(len(files)) / float64(s3_reader.EstimateFilesLimit)
		sniffFiles = files[:s3_reader.EstimateFilesLimit]
		lgr.Infof("there are too many files: %v, will sniff: %v files and multiply result on %v", len(files), s3_reader.EstimateFilesLimit, multiplier)
	}

	lgr.Infof("start to read: %v files in parallel", len(sniffFiles))

	sizes := make([]uint64, len(sniffFiles))
	if err := util.ParallelDo(
		ctx,
		len(sniffFiles),
		5,
		func(i int) error {
			file := sniffFiles[i]
			reader, err := readerCtor(ctx, *file.Key)
			if err != nil {
				return xerrors.Errorf("unable to open reader for file: %s, err: %w", *file.Key, err)
			}
			defer reader.Close()

			size := reader.Size()
			if size > 0 {
				sampleReader = reader
			}
			sizes[i] = uint64(size)
			return nil
		},
	); err != nil {
		return 0, sampleReader, xerrors.Errorf("unable to estimate size: %w", err)
	}

	var totalSize uint64
	for _, currFileSize := range sizes {
		totalSize += currFileSize
	}
	totalSize = uint64(float64(totalSize) * multiplier)

	if multiplier > 1 {
		lgr.Infof("size estimated: %v", format.SizeUInt64(totalSize))
	} else {
		lgr.Infof("size resolved: %v", format.SizeUInt64(totalSize))
	}

	return totalSize, sampleReader, nil
}
