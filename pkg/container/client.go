package container

import (
	"context"
	"io"

	docker_types "github.com/docker/docker/api/types"
	docker_container "github.com/docker/docker/api/types/container"
	docker_image "github.com/docker/docker/api/types/image"
	docker_network "github.com/docker/docker/api/types/network"
	opencontainers_specs "github.com/opencontainers/image-spec/specs-go/v1"
	k8s_api "k8s.io/api/core/v1"
	k8s_api_meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DockerClient interface {
	ImageInspectWithRaw(ctx context.Context, image string) (docker_types.ImageInspect, []byte, error)
	ImagePull(ctx context.Context, ref string, options docker_image.PullOptions) (io.ReadCloser, error)
	ContainerCreate(ctx context.Context, config *docker_container.Config, hostConfig *docker_container.HostConfig,
		networkingConfig *docker_network.NetworkingConfig, platform *opencontainers_specs.Platform, containerName string) (docker_container.CreateResponse, error)
	ContainerStart(ctx context.Context, containerID string, options docker_container.StartOptions) error
	ContainerAttach(ctx context.Context, containerID string, options docker_container.AttachOptions) (docker_types.HijackedResponse, error)
	ContainerWait(ctx context.Context, containerID string, condition docker_container.WaitCondition) (<-chan docker_container.WaitResponse, <-chan error)
	Ping(ctx context.Context) (docker_types.Ping, error)
	ContainerKill(ctx context.Context, containerID, signal string) error
}

type KubernetesClient interface {
	CreatePod(ctx context.Context, namespace string, pod *k8s_api.Pod) (*k8s_api.Pod, error)
	GetPodLogs(ctx context.Context, namespace, podName, containerName string, opts *k8s_api.PodLogOptions) (io.ReadCloser, error)
	GetPod(ctx context.Context, namespace, podName string) (*k8s_api.Pod, error)
	DeletePod(ctx context.Context, namespace, podName string, opts *k8s_api_meta.DeleteOptions) error
}
