//go:build disable_kubernetes

package container

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewK8sWrapper(logger log.Logger) (ContainerImpl, error) {
	return nil, xerrors.New("Kubernetes support is disabled in this build")
}
