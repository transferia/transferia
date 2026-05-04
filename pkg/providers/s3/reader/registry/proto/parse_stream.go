package proto

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
)

//nolint:descriptiveerrors
func streamParseFile(
	ctx context.Context,
	r *ProtoReader,
	filePath string,
	chunkReader *s3_reader.ChunkReader,
	pusher s3_pusher.Pusher,
	lastModified time.Time,
) reader_error.ReaderError {
	lastUnparsedData := make([]byte, 0)
	parser := r.parserBuilder.BuildBaseParser()
	for !chunkReader.IsEOF() {
		if ctx.Err() != nil {
			r.logger.Infof("stream parse file %s canceled", filePath)
			break
		}
		if err := chunkReader.ReadNextChunk(); err != nil {
			return reader_error.NewReaderErrorTransport("proto.streamParseFile.chunk.ReadNextChunk", filePath, err)
		}
		data := chunkReader.Data()
		parsed := parser.Do(constructMessage(lastModified, data, []byte(filePath)), abstract.NewEmptyPartition())
		if len(parsed) == 0 {
			continue
		}
		if unparsed := parsed[len(parsed)-1]; parsers.IsUnparsed(unparsed) {
			lastUnparsedData = data[len(data)-int(unparsed.Size.Read):]
			parsed = parsed[:len(parsed)-1]
		} else {
			lastUnparsedData = nil
		}
		parsedDataSize := int64(len(data) - len(lastUnparsedData))
		if r.unparsedPolicy == s3_model.UnparsedPolicyFail {
			if err := parsers.VerifyUnparsed(parsed...); err != nil {
				return reader_error.NewReaderErrorFatal("unable to parse", xerrors.Errorf("err: %w", err))
			}
		} else if r.unparsedPolicy == s3_model.UnparsedPolicyRetry {
			if err := parsers.VerifyUnparsed(parsed...); err != nil {
				return reader_error.NewReaderErrorDataFile("proto.streamParseFile.VerifyUnparsed.retry", filePath, err)
			}
		}
		if err := s3_reader.FlushChunk(ctx, filePath, uint64(chunkReader.Offset()), parsedDataSize, parsed, pusher); err != nil {
			return wrapProtoStreamParseFlush("proto.streamParseFile.FlushChunk", err)
		}
		chunkReader.FillBuffer(lastUnparsedData)
	}

	if len(lastUnparsedData) != 0 {
		switch r.unparsedPolicy {
		case s3_model.UnparsedPolicyFail:
			return reader_error.NewReaderErrorFatal("unparsed data found in the end of file", xerrors.Errorf("err: %s", filePath))
		case s3_model.UnparsedPolicyRetry:
			return reader_error.NewReaderErrorDataFile("proto.streamParseFile.tailUnparsedRetry", filePath, xerrors.New("unparsed data at end of file (retry)"))
		}
	}

	if len(lastUnparsedData) != 0 {
		data := chunkReader.Data()
		parsed := parser.Do(constructMessage(lastModified, data, []byte(filePath)), abstract.NewEmptyPartition())
		if err := s3_reader.FlushChunk(ctx, filePath, uint64(chunkReader.Offset()), int64(len(data)), parsed, pusher); err != nil {
			return wrapProtoStreamParseFlush("proto.streamParseFile.FlushChunk.tail", err)
		}
	}

	return nil
}

func wrapProtoStreamParseFlush(step string, err reader_error.ReaderError) reader_error.ReaderError {
	return reader_error.WrapReaderError(step, err)
}
