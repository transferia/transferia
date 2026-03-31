package container

import (
	"context"
	"io"

	docker_types "github.com/docker/docker/api/types"
	docker_container "github.com/docker/docker/api/types/container"
	docker_image "github.com/docker/docker/api/types/image"
	docker_network "github.com/docker/docker/api/types/network"
	opencontainers_specs "github.com/opencontainers/image-spec/specs-go/v1"
	testify_mock "github.com/stretchr/testify/mock"
)

type MockDockerClient struct {
	testify_mock.Mock
}

func (m *MockDockerClient) ImageInspectWithRaw(ctx context.Context, image string) (docker_types.ImageInspect, []byte, error) {
	args := m.Called(ctx, image)
	return args.Get(0).(docker_types.ImageInspect), args.Get(1).([]byte), args.Error(2)
}

func (m *MockDockerClient) ImagePull(ctx context.Context, ref string, options docker_image.PullOptions) (io.ReadCloser, error) {
	args := m.Called(ctx, ref, options)
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (m *MockDockerClient) ContainerCreate(ctx context.Context, config *docker_container.Config, hostConfig *docker_container.HostConfig,
	networkingConfig *docker_network.NetworkingConfig, platform *opencontainers_specs.Platform, containerName string,
) (docker_container.CreateResponse, error) {
	args := m.Called(ctx, config, hostConfig, networkingConfig, platform, containerName)
	return args.Get(0).(docker_container.CreateResponse), args.Error(1)
}

func (m *MockDockerClient) ContainerStart(ctx context.Context, containerID string, options docker_container.StartOptions) error {
	args := m.Called(ctx, containerID, options)
	return args.Error(0)
}

func (m *MockDockerClient) ContainerAttach(ctx context.Context, containerID string, options docker_container.AttachOptions) (docker_types.HijackedResponse, error) {
	args := m.Called(ctx, containerID, options)
	return args.Get(0).(docker_types.HijackedResponse), args.Error(1)
}

func (m *MockDockerClient) ContainerWait(ctx context.Context, containerID string, condition docker_container.WaitCondition) (<-chan docker_container.WaitResponse, <-chan error) {
	args := m.Called(ctx, containerID, condition)
	return args.Get(0).(<-chan docker_container.WaitResponse), args.Get(1).(<-chan error)
}

func (m *MockDockerClient) Ping(ctx context.Context) (docker_types.Ping, error) {
	args := m.Called(ctx)
	return args.Get(0).(docker_types.Ping), args.Error(1)
}

func (m *MockDockerClient) ContainerKill(ctx context.Context, containerID, signal string) error {
	args := m.Called(ctx, containerID, signal)
	return args.Error(0)
}
