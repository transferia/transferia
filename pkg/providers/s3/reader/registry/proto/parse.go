package proto

import (
	"context"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
)

const perPushBatchSize = 15 * humanize.MiByte

//nolint:descriptiveerrors
func readFileAndParse(ctx context.Context, r *ProtoReader, schema *abstract.TableSchema, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	if schema == nil || len(schema.Columns()) == 0 {
		return nil
	}

	s3RawReader, err2 := r.newS3RawReader(ctx, filePath)
	if err2 != nil {
		return reader_error.NewReaderErrorFromObjectOpen("proto.readFileAndParse.newS3RawReader", filePath, err2)
	}

	if s3RawReader.Size() > int64(perPushBatchSize) {
		chunkReader := s3_reader.NewChunkReader(s3RawReader, perPushBatchSize, r.logger)
		defer chunkReader.Close()

		return reader_error.WrapReaderError("proto.readFileAndParse.streamParseFile", streamParseFile(ctx, r, filePath, chunkReader, pusher, s3RawReader.LastModified()))
	}

	fullFile, err := s3raw.ReadWholeFile(ctx, s3RawReader, r.blockSize)
	if err != nil {
		return reader_error.NewReaderErrorTransport("proto.readFileAndParse.ReadWholeFile", filePath, err)
	}
	msg := constructMessage(s3RawReader.LastModified(), fullFile, []byte(filePath))
	parser, err := r.parserBuilder.BuildLazyParser(msg, abstract.NewEmptyPartition())
	if err != nil {
		return reader_error.NewReaderErrorDataFile("proto.readFileAndParse.build_parser", filePath, err)
	}

	var buff []abstract.ChangeItem
	buffSize := int64(0)

	for item := parser.Next(); item != nil; item = parser.Next() {
		if parsers.IsUnparsed(*item) {
			switch r.unparsedPolicy {
			case s3_model.UnparsedPolicyFail:
				if err := parsers.VerifyUnparsed(*item); err != nil {
					return reader_error.NewReaderErrorFatal("unable to parse", xerrors.Errorf("err: %w", err))
				}
			case s3_model.UnparsedPolicyRetry:
				if err := parsers.VerifyUnparsed(*item); err != nil {
					return reader_error.NewReaderErrorDataFile("proto.readFileAndParse.unparsedRetry", filePath, err)
				}
			}
		}
		buff = append(buff, *item)
		buffSize += int64(item.Size.Read)
		if item.Size.Read == 0 {
			r.logger.Warn("Got item with 0 raw read size")
			buffSize += 64 * humanize.KiByte
		}
		if buffSize > perPushBatchSize {
			if err := s3_reader.FlushChunk(ctx, filePath, 0, buffSize, buff, pusher); err != nil {
				return wrapProtoReadFileFlushChunk(err)
			}
			buff = nil
			buffSize = 0
		}
	}

	if len(buff) != 0 {
		if err := s3_reader.FlushChunk(ctx, filePath, 0, buffSize, buff, pusher); err != nil {
			return wrapProtoReadFileFlushChunk(err)
		}
	}
	return nil
}

func wrapProtoReadFileFlushChunk(err reader_error.ReaderError) reader_error.ReaderError {
	return reader_error.WrapReaderError("proto.readFileAndParse.FlushChunk", err)
}
