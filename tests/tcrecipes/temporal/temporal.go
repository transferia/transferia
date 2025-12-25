package tctemporal

import (
	"context"
	"fmt"
	"os"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/transferia/transferia/library/go/test/yatest"
)

const (
	URLEnvVar = "TEMPORAL_URI"
	webUIPort = nat.Port("8233/tcp")
	apiPort   = nat.Port("7233/tcp")
)

type TemporalContainer struct {
	testcontainers.Container
	url   string
	webUI string
}

func (tc TemporalContainer) ConnectionURL() string {
	return tc.url
}

func (tc TemporalContainer) WebUI() string {
	return tc.webUI
}

func Prepare(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*TemporalContainer, error) {
	if os.Getenv("USE_TESTCONTAINERS") != "1" {
		return nil, nil
	}

	req := testcontainers.ContainerRequest{
		Name: "temporal",
		FromDockerfile: testcontainers.FromDockerfile{
			Context:       yatest.SourcePath("cloud/dataplatform/testcontainer/temporal"),
			PrintBuildLog: true,
		},
		ExposedPorts: []string{webUIPort.Port(), apiPort.Port()},
		WaitingFor:   wait.ForListeningPort(apiPort),
		Cmd:          []string{},
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	for _, opt := range opts {
		_ = opt.Customize(&genericContainerReq)
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, err
	}

	exposedPort, err := container.MappedPort(ctx, apiPort)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("localhost:%s", exposedPort.Port())
	if err := os.Setenv(URLEnvVar, url); err != nil {
		return nil, err
	}

	exposedPort, err = container.MappedPort(ctx, webUIPort)
	if err != nil {
		return nil, err
	}

	webUI := fmt.Sprintf("http://localhost:%s", exposedPort.Port())

	return &TemporalContainer{
		Container: container,
		url:       url,
		webUI:     webUI,
	}, nil
}

func WithNamespace(namespace string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.ContainerRequest.Cmd = append(req.ContainerRequest.Cmd, "--namespace", namespace)

		return nil
	}
}
