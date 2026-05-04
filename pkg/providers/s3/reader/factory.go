package reader

import (
	"sort"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	xmaps "golang.org/x/exp/maps"
)

// registered reader implementations by model.ParsingFormat
var readerImpls = make(map[model.ParsingFormat]func(src *s3_model.S3Source, lgr log.Logger, sess *aws_session.Session, metrics *stats.SourceStats, s3RawReaderBuilder s3raw.S3RawReaderBuilder) (S3Reader, S3SchemaResolver, error))

// NewS3Reader builds the format-specific reader
type NewS3Reader func(src *s3_model.S3Source, lgr log.Logger, sess *aws_session.Session, metrics *stats.SourceStats, s3RawReaderBuilder s3raw.S3RawReaderBuilder) (S3Reader, error)

// NewSchemaResolver builds the format-specific S3SchemaResolver (preset + optional S3 listing; see ExecuteSchemaResolver).
type NewSchemaResolver func(src *s3_model.S3Source, lgr log.Logger, sess *aws_session.Session, metrics *stats.SourceStats, s3RawReaderBuilder s3raw.S3RawReaderBuilder) (S3SchemaResolver, error)

func RegisterReader(format model.ParsingFormat, newRead NewS3Reader, newSchema NewSchemaResolver) {
	readerImpls[format] = func(src *s3_model.S3Source, lgr log.Logger, sess *aws_session.Session, metrics *stats.SourceStats, s3RawReaderBuilder s3raw.S3RawReaderBuilder) (S3Reader, S3SchemaResolver, error) {
		read, err := newRead(src, lgr, sess, metrics, s3RawReaderBuilder)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to initialize registry reader for format %s, err: %w", format, err)
		}
		sch, err := newSchema(src, lgr, sess, metrics, s3RawReaderBuilder)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to initialize schema resolver for format %s, err: %w", format, err)
		}
		return read, sch, nil
	}
}

func newImpl(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (S3Reader, S3SchemaResolver, error) {
	ctor, ok := readerImpls[src.InputFormat]
	if !ok {
		return nil, nil, xerrors.Errorf("unknown format: %s", src.InputFormat)
	}
	return ctor(src, lgr, sess, metrics, s3RawReaderBuilder)
}

func New(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (Reader, error) {
	read, schema, err := newImpl(src, lgr, sess, metrics, s3RawReaderBuilder)
	if err != nil {
		return nil, xerrors.Errorf("unable to create new reader: %w", err)
	}
	return NewReaderContractor(
		read,
		schema,
		withSchemaBackoff(DefaultContractorBackoff()),
	), nil
}

func RegisteredFormats() []string {
	keys := xmaps.Keys(readerImpls)
	keysStrArr := yslices.Map(keys, func(t model.ParsingFormat) string { return string(t) })
	sort.Strings(keysStrArr)
	return keysStrArr
}
