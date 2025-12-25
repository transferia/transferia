package s3raw

import (
	"context"
	"io"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// calcRange calculates range ([begin, end], inclusive) to iterate over p starting with offset.
// It is guaranteed that end < totalSize.
func calcRange(bufferSize, offset, totalSize int64) (int64, int64, error) {
	if offset < 0 {
		return 0, 0, xerrors.New("offset is negative")
	}
	if totalSize <= 0 {
		return 0, 0, xerrors.New("totalSize is negative or zero")
	}
	if offset >= totalSize {
		return 0, 0, xerrors.New("offset is bigger than totalSize")
	}
	if bufferSize == 0 {
		return 0, 0, xerrors.New("size of p is zero")
	}

	start := offset
	end := offset + int64(bufferSize) - 1
	if end < totalSize {
		return start, end, nil
	}
	return start, totalSize - 1, io.EOF
}

// ReadWholeFile calls reader.ReadAll if implemented, or readAllByBlocks otherwise.
func ReadWholeFile(ctx context.Context, reader S3RawReader, blockSize int64) ([]byte, error) {
	if allReader, ok := reader.(ReaderAll); ok {
		return allReader.ReadAll()
	}
	res, err := readAllByBlocks(ctx, reader, blockSize)
	if err != nil {
		return nil, xerrors.Errorf("unable to read all by blocks: %w", err)
	}
	return res, nil
}

// readAllByBlocks reads all data from reader using for-loop calls of ReadAt method.
func readAllByBlocks(ctx context.Context, reader S3RawReader, blockSize int64) ([]byte, error) {
	offset := 0
	fullFile := make([]byte, 0, reader.Size())
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("GenericParserReader readAllByBlocks canceled")
			return nil, nil
		default:
		}
		data := make([]byte, blockSize)
		lastRound := false
		n, err := reader.ReadAt(data, int64(offset))
		if err != nil {
			if xerrors.Is(err, io.EOF) && n > 0 {
				data = data[0:n]
				lastRound = true
			} else {
				return nil, xerrors.Errorf("failed to read from file: %w", err)
			}
		}
		offset += n

		fullFile = append(fullFile, data...)
		if lastRound {
			break
		}
	}
	return fullFile, nil
}
