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

// ContainerBackend represents the type of container backend
type ContainerBackend string

const (
	// BackendDocker represents Docker container backend
	BackendDocker ContainerBackend = "docker"
	// BackendKubernetes represents Kubernetes container backend
	BackendKubernetes ContainerBackend = "kubernetes"
)

// Container defines container operations
type ContainerImpl interface {
	// Run executes the container/pod process in the background.
	// It returns immediately with io.ReadCloser streams for stdout and stderr.
	// The readers will return io.EOF when the underlying process completes.
	// The caller is responsible for closing the streams.
	Run(ctx context.Context, options ContainerOpts) (stdout io.ReadCloser, stderr io.ReadCloser, err error)

	// It captures the entire stdout and stderr output in memory buffers, which can
	// potentially consume significant memory for processes with large outputs.
	// Callers should use this method only when output size is expected to be reasonable.
	// For processes with large or unbounded output, consider using Run() with streaming
	// instead to avoid memory exhaustion.
	// Returns the captured output buffers and any error encountered during execution.
	RunAndWait(ctx context.Context, options ContainerOpts) (stdout *bytes.Buffer, stderr *bytes.Buffer, err error)

	// Pull pulls a container image
	Pull(ctx context.Context, image string, opts types.ImagePullOptions) error

	// Type returns the container backend type
	Type() ContainerBackend
}

func NewContainerImpl(l log.Logger) (ContainerImpl, error) {
	if isRunningInKubernetes() {
		k8sClient, err := NewK8sWrapper(l)
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

// isRunningInKubernetes checks if the code is running in a Kubernetes environment
// by looking for the KUBERNETES_SERVICE_HOST environment variable
func isRunningInKubernetes() bool {
	_, exists := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	return exists
}
