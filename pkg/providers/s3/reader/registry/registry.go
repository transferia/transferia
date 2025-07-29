package registry

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/csv"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/json"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/line"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/parquet"
	_ "github.com/transferia/transferia/pkg/providers/s3/reader/registry/proto"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewReader(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (reader.Reader, error) {
	return reader.New(src, lgr, sess, metrics)
}
