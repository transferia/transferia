package estimators

import (
	"context"
	"io"
	"math"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"go.ytsaurus.tech/library/go/core/log"
)

func EstimateRows(
	ctx context.Context,
	inLogger log.Logger,
	files []*aws_s3.Object,
	blockSize int,
	newS3RawReader ReaderCtorF,
	readLinesFunc func([]byte) ([]string, uint64, error),
) (uint64, error) {
	totalSize, sampleReader, err := EstimateTotalSizeInBytes(ctx, inLogger, files, newS3RawReader)
	if err != nil {
		return 0, xerrors.Errorf("unable to load file list, err: %w", err)
	}

	if totalSize == 0 {
		if sampleReader != nil {
			_ = sampleReader.Close()
		}
		return 0, nil
	}

	if sampleReader == nil {
		return 0, nil
	}

	chunkReader := s3_reader.NewChunkReader(sampleReader, blockSize, inLogger)
	defer chunkReader.Close()

	err = chunkReader.ReadNextChunk()
	if err != nil && !xerrors.Is(err, io.EOF) {
		return 0, xerrors.Errorf("unable to read chunk, err: %w", err)
	}
	data := chunkReader.Data()
	if len(data) == 0 {
		return 0, nil
	}
	lines, bytesRead, err := readLinesFunc(data)
	if err != nil {
		return 0, xerrors.Errorf("unable to read lines, err: %w", err)
	}
	if len(lines) == 0 {
		return 0, nil
	}
	bytesPerLine := float64(bytesRead) / float64(len(lines))
	totalLines := math.Ceil(float64(totalSize) / bytesPerLine)
	return uint64(totalLines), nil
}
