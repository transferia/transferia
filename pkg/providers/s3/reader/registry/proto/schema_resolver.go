package proto

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	abstract_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/util"
)

func resolveSchema(ctx context.Context, r *ProtoReader, key string) (*abstract.TableSchema, error) {
	s3RawReader, err := r.newS3RawReader(ctx, key)
	if err != nil {
		return nil, xerrors.Errorf("unable to open reader for file: %s: %w", key, err)
	}

	chunkReader := abstract_reader.NewChunkReader(s3RawReader, int(r.blockSize), r.logger)
	defer chunkReader.Close()
	err = chunkReader.ReadNextChunk()
	if err != nil && !xerrors.Is(err, io.EOF) {
		return nil, xerrors.Errorf("failed to read sample from file: %s: %w", key, err)
	}
	if len(chunkReader.Data()) == 0 {
		return nil, xerrors.New(fmt.Sprintf("could not read sample data from file: %s", key))
	}

	reader := bufio.NewReader(bytes.NewReader(chunkReader.Data()))
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, xerrors.Errorf("failed to read sample content for schema deduction: %w", err)
	}
	parser, err := r.parserBuilder.BuildLazyParser(constructMessage(s3RawReader.LastModified(), content, []byte(key)), abstract.NewEmptyPartition())
	if err != nil {
		return nil, xerrors.Errorf("failed to prepare parser: %w", err)
	}
	item := parser.Next()
	if item == nil {
		return nil, xerrors.Errorf("unable to parse sample data: %v", util.Sample(string(content), 1024))
	}
	r.tableSchema = item.TableSchema
	return r.tableSchema, nil
}
