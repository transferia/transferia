//go:build disable_etcd_coordinator

package etcdcoordinator

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"go.ytsaurus.tech/library/go/core/log"
)

type EtcdConfig struct {
	Endpoints   []string
	DialTimeout time.Duration
	Username    string
	Password    string
	CertFile    string
	KeyFile     string
	CAFile      string
}

func NewEtcdCoordinator(ctx context.Context, config EtcdConfig, logger log.Logger) (*EtcdCoordinator, error) {
	return nil, xerrors.New("Etcd coordinator is not available in this build")
}

type EtcdCoordinator struct {
	*coordinator.CoordinatorNoOp
}
