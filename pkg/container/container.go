package container

import (
	"context"
	"io"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type ContainerImpl interface {
	Run(context.Context, ContainerOpts) (io.Reader, io.Reader, error)
	Pull(context.Context, string, types.ImagePullOptions) error
}

func NewContainerImpl(l log.Logger) (ContainerImpl, error) {
	if isRunningInKubernetes() {
		k8sClient, err := NewK8sWrapper()
		if err != nil {
			return nil, xerrors.Errorf("unable to init k8s wrapper: %w", err)
		}
		return k8sClient, nil
	}
	dockerClient, err := NewDockerWrapper(l)
	if err != nil {
		return nil, xerrors.Errorf("unable to init docker wrapper: %w", err)
	}
	return dockerClient, nil
}

func isRunningInKubernetes() bool {
	_, exists := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	return exists
}
