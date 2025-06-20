//go:build disable_s3_coordinator

package s3coordinator

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"go.ytsaurus.tech/library/go/core/log"
)

type CoordinatorS3 struct {
	*coordinator.CoordinatorNoOp
}

func NewS3(bucket string, l log.Logger, cfgs ...*aws.Config) (*CoordinatorS3, error) {
	return nil, xerrors.New("S3 coordinator is not available in this build")
}
