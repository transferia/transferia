package s3raw

import (
	"bytes"
	"context"
	"io"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
)

// CoalesceS3RawReaderBuilder returns b when non-nil, otherwise NewRealS3RawReaderBuilder().
// Unit tests and legacy struct literals may omit the builder; production paths pass an explicit builder.
func CoalesceS3RawReaderBuilder(b S3RawReaderBuilder) S3RawReaderBuilder {
	if b != nil {
		return b
	}
	return NewRealS3RawReaderBuilder()
}

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
	end := offset + bufferSize - 1
	if end < totalSize {
		return start, end, nil
	}
	return start, totalSize - 1, io.EOF
}

// ReadWholeFile calls reader.ReadAll if implemented, or readAllByBlocks otherwise.
func ReadWholeFile(ctx context.Context, reader S3RawReader, blockSize int64) ([]byte, error) {
	if allReader, ok := reader.(ReaderAll); ok {
		return allReader.ReadAll(ctx)
	}
	res, err := readAllByBlocks(ctx, reader, blockSize)
	if err != nil {
		return nil, xerrors.Errorf("failed to readAllByBlocks, err: %w", err)
	}
	return res, nil
}

// readAllByBlocks reads all data from reader using for-loop calls of ReadAt method.
func readAllByBlocks(ctx context.Context, reader S3RawReader, blockSize int64) ([]byte, error) {
	offset := 0
	fullFile := make([]byte, 0, int(reader.Size()))
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("GenericParserReader readAllByBlocks canceled")
			return nil, ctx.Err()
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
				return nil, xerrors.Errorf("failed to ReadAt, err: %w", err)
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

func newFakeS3RawReader(fakeS3Client *fake_s3.FakeS3Client) (*FakeS3RawReader, error) {
	if fakeS3Client == nil {
		return nil, xerrors.New("fake s3 client is nil")
	}

	file, err := fakeS3Client.GetSingleFile()
	if err != nil {
		return nil, err
	}

	return &FakeS3RawReader{
		file:   file,
		reader: bytes.NewReader(file.Body),
	}, nil
}
