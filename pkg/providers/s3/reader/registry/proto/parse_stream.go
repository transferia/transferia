package proto

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/providers/s3"
	chunk_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	abstract_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
)

func streamParseFile(ctx context.Context, r *ProtoReader, filePath string, chunkReader *abstract_reader.ChunkReader, pusher chunk_pusher.Pusher, lastModified time.Time) error {
	lastUnparsedData := make([]byte, 0)
	parser := r.parserBuilder.BuildBaseParser()
	for !chunkReader.IsEOF() {
		if ctx.Err() != nil {
			r.logger.Infof("stream parse file %s canceled", filePath)
			break
		}
		if err := chunkReader.ReadNextChunk(); err != nil {
			return xerrors.Errorf("failed to read sample from file: %s: %w", filePath, err)
		}
		data := chunkReader.Data()
		parsed := parser.Do(constructMessage(lastModified, data, []byte(filePath)), abstract.NewPartition(filePath, 0))
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
		if r.unparsedPolicy == s3.UnparsedPolicyFail {
			if err := parsers.VerifyUnparsed(parsed...); err != nil {
				return abstract.NewFatalError(xerrors.Errorf("unable to parse: %w", err))
			}
		}
		if err := abstract_reader.FlushChunk(ctx, filePath, uint64(chunkReader.Offset()), parsedDataSize, parsed, pusher); err != nil {
			return xerrors.Errorf("unable to push: %w", err)
		}
		chunkReader.FillBuffer(lastUnparsedData)
	}

	if len(lastUnparsedData) > 0 && r.unparsedPolicy == s3.UnparsedPolicyFail {
		return abstract.NewFatalError(xerrors.Errorf("unparsed data found in the end of file: %s", filePath))
	}

	if len(lastUnparsedData) > 0 {
		data := chunkReader.Data()
		parsed := parser.Do(constructMessage(lastModified, data, []byte(filePath)), abstract.NewPartition(filePath, 0))
		if err := abstract_reader.FlushChunk(ctx, filePath, uint64(chunkReader.Offset()), int64(len(data)), parsed, pusher); err != nil {
			return xerrors.Errorf("unable to push: %w", err)
		}
	}

	return nil
}
