//go:build disable_docker

package container

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewDockerWrapper(logger log.Logger) (ContainerImpl, error) {
	return nil, xerrors.New("Docker support is disabled in this build")
}
