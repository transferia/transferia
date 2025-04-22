package container

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

// Container defines container operations
type ContainerImpl interface {
	// Run executes the container/pod process in the background.
	// It returns immediately with io.ReadCloser streams for stdout and stderr.
	// The readers will return io.EOF when the underlying process completes.
	// Implementations should ensure proper handling of context cancellation.
	Run(ctx context.Context, options ContainerOpts) (stdout io.ReadCloser, stderr io.ReadCloser, err error)

	// RunAndWait executes the container/pod process and blocks until completion.
	// It captures the full stdout and stderr streams into buffers.
	// Returns the captured output and any error encountered during execution or capture.
	RunAndWait(ctx context.Context, options ContainerOpts) (stdout *bytes.Buffer, stderr *bytes.Buffer, err error)

	// Pull pulls a container image
	Pull(ctx context.Context, image string, opts types.ImagePullOptions) error
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
