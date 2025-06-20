//go:build !disable_yt_provider

package recipe

import (
	"context"
	"fmt"
	"net"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

const (
	defaultImage    = "ghcr.io/ytsaurus/local-nightly:dev-2024-10-16-50e2ea53cfec3c9973e5b065f839e05a73506945"
	containerPort   = "80/tcp"
	DefaultUser     = "admin"
	DefaultPassword = "password"
	DefaultToken    = "password"
)

// YTsaurusContainer represents the YTsaurus container type used in the module.
type YTsaurusContainer struct {
	testcontainers.Container
}

// ConnectionHost returns the host and dynamic port for accessing the YTsaurus container.
func (y *YTsaurusContainer) ConnectionHost(ctx context.Context) (string, error) {
	host, err := y.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("get host: %w", err)
	}

	mappedPort, err := y.MappedPort(ctx, containerPort)
	if err != nil {
		return "", fmt.Errorf("get mapped port: %w", err)
	}

	return fmt.Sprintf("%s:%s", host, mappedPort.Port()), nil
}

// GetProxy is an alias for ConnectionHost since `proxy` is more familiar term for in YTsaurus.
func (y *YTsaurusContainer) GetProxy(ctx context.Context) (string, error) {
	return y.ConnectionHost(ctx)
}

// Token returns the token for the YTsaurus container.
func (y *YTsaurusContainer) Token() string {
	return "password"
}

// NewClient creates a new YT client connected to the YTsaurus container.
func (y *YTsaurusContainer) NewClient(ctx context.Context) (yt.Client, error) {
	host, err := y.ConnectionHost(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection host: %w", err)
	}

	client, err := ythttp.NewClient(&yt.Config{
		Proxy: host,
		Credentials: &yt.TokenCredentials{
			Token: y.Token(),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create YT client: %w", err)
	}
	return client, nil
}

// WithAuth enables authentication on http proxies and creates `admin` user with password and token `password`.
func WithAuth() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Cmd = append(
			req.Cmd,
			"--native-client-supported", // required by yt_python for auth setup
			"--enable-auth",
			"--create-admin-user",
		)
		return nil
	}
}

// RunContainer creates and starts an instance of the YTsaurus container.
func RunContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*YTsaurusContainer, error) {
	randomPort, err := getFreePort()
	if err != nil {
		return nil, fmt.Errorf("get random free port: %w", err)
	}

	req := testcontainers.ContainerRequest{
		Image:        defaultImage,
		ExposedPorts: []string{fmt.Sprintf("%d:%s", randomPort, containerPort)},
		WaitingFor:   wait.ForLog("Local YT started"),
		Cmd: []string{
			"--fqdn",
			"localhost",
			"--proxy-config",
			fmt.Sprintf("{address_resolver={enable_ipv4=%%true;enable_ipv6=%%false;};coordinator={public_fqdn=\"localhost:%d\"}}", randomPort),
			"--enable-debug-logging",
			"--wait-tablet-cell-initialization",
		},
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	for _, opt := range opts {
		if err := opt.Customize(&genericContainerReq); err != nil {
			return nil, xerrors.Errorf("customize container request: %w", err)
		}
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, xerrors.Errorf("start container: %w", err)
	}

	return &YTsaurusContainer{Container: container}, nil
}

func getFreePort() (port int, err error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, xerrors.Errorf("unabel to parse addr: %w", err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, xerrors.Errorf("unable to listen: %w", err)
	}
	defer func() {
		if closeErr := listener.Close(); closeErr != nil {
			err = fmt.Errorf("close listener: %w", err)
		}
	}()

	return listener.Addr().(*net.TCPAddr).Port, nil
}
