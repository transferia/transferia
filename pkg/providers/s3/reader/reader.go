package reader

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

// registred reader implementations by model.ParsingFormat
var readerImpls = map[model.ParsingFormat]func(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (Reader, error){}

type NewReader func(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (Reader, error)

func RegisterReader(format model.ParsingFormat, ctor NewReader) {
	wrappedCtor := func(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (Reader, error) {
		reader, err := ctor(src, lgr, sess, metrics)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new reader for format %s: %w", format, err)
		}
		return reader, nil
	}

	readerImpls[format] = wrappedCtor
}

func newImpl(
	src *s3.S3Source,
	lgr log.Logger,
	sess *session.Session,
	metrics *stats.SourceStats,
) (Reader, error) {
	ctor, ok := readerImpls[src.InputFormat]
	if !ok {
		return nil, xerrors.Errorf("unknown format: %s", src.InputFormat)
	}
	return ctor(src, lgr, sess, metrics)
}

func New(
	src *s3.S3Source,
	lgr log.Logger,
	sess *session.Session,
	metrics *stats.SourceStats,
) (Reader, error) {
	result, err := newImpl(src, lgr, sess, metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create new reader: %w", err)
	}
	return NewReaderContractor(result), nil
}
