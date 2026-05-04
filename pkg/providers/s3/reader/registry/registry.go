package registry

import (
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/csv"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/json"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/line"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/nginx"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/parquet"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/proto"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

// NewReader delegates construction to s3_reader.New for registered parsing formats.
func NewReader(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.Reader, error) {
	return s3_reader.New(src, lgr, sess, metrics, s3RawReaderBuilder)
}

func RegisteredFormats() []string {
	return s3_reader.RegisteredFormats()
}
