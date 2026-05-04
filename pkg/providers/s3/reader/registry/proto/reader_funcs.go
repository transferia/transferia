package proto

import (
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func newReaderImpl(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	parserBuilder parsers.ParserBuilder,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
	schemaResolver s3_reader.S3SchemaResolver,
) (*ProtoReader, error) {
	reader := &ProtoReader{
		s3RawReaderBuilder: s3RawReaderBuilder,
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		bucket:         src.Bucket,
		client:         s3RawReaderBuilder.BuildClient(sess),
		logger:         lgr,
		pathPrefix:     src.PathPrefix,
		blockSize:      defaultBlockSize,
		pathPattern:    src.PathPattern,
		metrics:        metrics,
		parserBuilder:  parserBuilder,
		unparsedPolicy: src.UnparsedPolicy,
		schemaResolver: schemaResolver,
	}

	return reader, nil
}
