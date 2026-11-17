package localstack

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/mod/semver"
	"golang.org/x/xerrors"
)

const (
	defaultPort            = 4566
	hostnameExternalEnvVar = "HOSTNAME_EXTERNAL"
	localstackHostEnvVar   = "LOCALSTACK_HOST"
)

func isVersion2(image string) bool {
	parts := strings.Split(image, ":")
	version := parts[len(parts)-1]

	if version == "latest" {
		return true
	}

	if !strings.HasPrefix(version, "v") {
		version = fmt.Sprintf("v%s", version)
	}

	if semver.IsValid(version) {
		return semver.Compare(version, "v2.0") > 0 // version >= v2.0
	}

	return true
}

func GetEndpoint(ls *Container, ctx context.Context) (string, error) {
	host := ls.host
	port, err := ls.MappedPort(ctx, nat.Port("4566/tcp"))
	if err != nil {
		return "", xerrors.Errorf(
			"Failed to retrieve the localstack container port %w", err)
	}

	return host + port.Port(), nil
}

// Run creates an instance of the LocalStack container type - overrideReq: a
// function that can be used to override the default container request, usually
// used to set the image version, environment variables for localstack, etc.
func Run(
	ctx context.Context,
	img string,
	opts ...testcontainers.ContainerCustomizer,
) (*Container, error) {
	req := testcontainers.ContainerRequest{
		Image: img,
		WaitingFor: wait.ForHTTP("/_localstack/health").
			WithPort("4566/tcp").WithStartupTimeout(120 * time.Second),
		ExposedPorts: []string{fmt.Sprintf("%d/tcp", defaultPort)},
		Env: map[string]string{
			"AWS_ACCESS_KEY_ID":     "AKID",
			"AWS_SECRET_ACCESS_KEY": "secretkey",
			"AWS_REGION":            "us-west-2",
		},
	}

	localStackReq := LocalStackContainerRequest{
		GenericContainerRequest: testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Logger:           testcontainers.Logger,
			Started:          true,
		},
	}

	for _, opt := range opts {
		if err := opt.Customize(
			&localStackReq.GenericContainerRequest,
		); err != nil {
			return nil, err
		}
	}

	envVar := hostnameExternalEnvVar
	if isVersion2(localStackReq.Image) {
		envVar = localstackHostEnvVar
	}

	hostnameExternalReason, err := configureDockerHost(&localStackReq, envVar)
	if err != nil {
		return nil, err
	}

	localStackReq.GenericContainerRequest.Logger.Printf(
		"Setting %s to %s (%s)\n",
		envVar,
		req.Env[envVar],
		hostnameExternalReason,
	)

	container, err := testcontainers.GenericContainer(
		ctx,
		localStackReq.GenericContainerRequest)
	if err != nil {
		return nil, err
	}

	c := &Container{
		Container: container,
		host:      "http://localhost:",
	}

	return c, nil
}

func configureDockerHost(
	req *LocalStackContainerRequest,
	envVar string,
) (string, error) {
	reason := ""

	if _, ok := req.Env[envVar]; ok {
		return "explicitly as environment variable", nil
	}

	// if the container is not connected to the default network, use the last
	// network alias in the first network for that we need to check if the
	// container is connected to a network and if it has network aliases
	if len(req.Networks) > 0 &&
		len(req.NetworkAliases) > 0 &&
		len(req.NetworkAliases[req.Networks[0]]) > 0 {
		alias :=
			req.NetworkAliases[req.Networks[0]][len(req.NetworkAliases[req.Networks[0]])-1]

		req.Env[envVar] = alias
		return "to match last network alias on container with non-default network", nil
	}

	dockerProvider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return reason, err
	}
	defer dockerProvider.Close()

	daemonHost, err := dockerProvider.DaemonHost(context.Background())
	if err != nil {
		return reason, err
	}

	req.Env[envVar] = daemonHost
	return "to match host-routable address for container", nil
}
