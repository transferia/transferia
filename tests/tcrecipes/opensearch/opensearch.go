package opensearch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const defaultOpenSearchImage = "opensearchproject/opensearch:2.8.0" // Sandbox resource: 12662827229
const defaultHTTPPort = nat.Port("9200/tcp")
const defaultTCPPort = nat.Port("9300/tcp")
const defaultClusterName = "opensearch-cluster"

type OpenSearchContainer struct {
	testcontainers.Container
	clusterName     string
	username        string
	password        string
	host            string
	exposedHTTPPort nat.Port
	exposedTCPPort  nat.Port
}

func (c *OpenSearchContainer) Host() string {
	return c.host
}

func (c *OpenSearchContainer) HTTPPort() int {
	p, _ := strconv.Atoi(c.exposedHTTPPort.Port())
	return p
}

func (c *OpenSearchContainer) TCPPort() int {
	p, _ := strconv.Atoi(c.exposedTCPPort.Port())
	return p
}

func (c *OpenSearchContainer) User() string {
	return c.username
}

func (c *OpenSearchContainer) Password() string {
	return c.password
}

func WithClusterName(name string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["cluster.name"] = name
		return nil
	}
}

func WithPlugins(plugins ...string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["OPENSEARCH_PLUGINS"] = strings.Join(plugins, ",")
		return nil
	}
}

func WithImage(image string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if image == "" {
			image = defaultOpenSearchImage
		}
		req.Image = image
		return nil
	}
}

func WithCustomDocker(docker testcontainers.FromDockerfile) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.FromDockerfile = docker
		req.Image = ""
		return nil
	}
}

func WithMemory(limit string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["OPENSEARCH_JAVA_OPTS"] = fmt.Sprintf("-Xms%s -Xmx%s", limit, limit)
		return nil
	}
}

func WithSingleNode() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["discovery.type"] = "single-node"
		return nil
	}
}

type hostNetworkOption struct {
	host string
}

func (o hostNetworkOption) Customize(req *testcontainers.GenericContainerRequest) error {
	req.Env["network.host"] = o.host
	// Performance Analyzer otherwise opens an additional wildcard listener on port 9600.
	req.Env["DISABLE_PERFORMANCE_ANALYZER_AGENT_CLI"] = "true"
	// testcontainers-go v0.32 waits for request-level exposed ports to have
	// published mappings. Host networking has no mappings, so omit them here;
	// MappedPort returns the original ports and the HTTP wait still checks readiness.
	req.ExposedPorts = nil
	// The Podman provider appends its default bridge to every request, even in host mode.
	req.ProviderType = testcontainers.ProviderDocker
	previous := req.HostConfigModifier
	req.HostConfigModifier = func(hostConfig *dockercontainer.HostConfig) {
		if previous != nil {
			previous(hostConfig)
		}
		hostConfig.NetworkMode = "host"
		hostConfig.PortBindings = nil
	}
	return nil
}

// WithHostNetwork runs OpenSearch in the host network namespace and binds it
// only to the supplied IPv4 loopback address.
func WithHostNetwork(host string) testcontainers.ContainerCustomizer {
	return hostNetworkOption{host: host}
}

func Prepare(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (_ *OpenSearchContainer, resultErr error) {
	req := testcontainers.ContainerRequest{
		Image: defaultOpenSearchImage,
		Env: map[string]string{
			"discovery.type":              "single-node",
			"cluster.name":                defaultClusterName,
			"bootstrap.memory_lock":       "true",
			"OPENSEARCH_JAVA_OPTS":        "-Xms512m -Xmx512m",
			"DISABLE_INSTALL_DEMO_CONFIG": "true",
			"DISABLE_SECURITY_PLUGIN":     "true",
		},
		ExposedPorts: []string{
			string(defaultHTTPPort),
			string(defaultTCPPort),
		},
		WaitingFor: wait.ForHTTP("/_cluster/health").
			WithPort(defaultHTTPPort).
			WithStatusCodeMatcher(func(status int) bool { return status == 200 }).
			WithStartupTimeout(120 * time.Second),
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	hostNetworkAddress := ""
	for _, opt := range opts {
		if option, ok := opt.(hostNetworkOption); ok {
			hostNetworkAddress = option.host
		}
		if err := opt.Customize(&genericContainerReq); err != nil {
			return nil, err
		}
	}
	if genericContainerReq.ContainerRequest.FromDockerfile.Dockerfile != "" {
		genericContainerReq.ContainerRequest.Image = ""
	}

	container, resultErr := testcontainers.GenericContainer(ctx, genericContainerReq)
	if container != nil {
		defer func() {
			if resultErr == nil {
				return
			}
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, container.Terminate(cleanupCtx))
		}()
	}
	if resultErr != nil {
		return nil, resultErr
	}

	clusterName := genericContainerReq.ContainerRequest.Env["cluster.name"]

	exposedHTTPPort, err := container.MappedPort(ctx, defaultHTTPPort)
	if err != nil {
		return nil, err
	}

	exposedTCPPort, err := container.MappedPort(ctx, defaultTCPPort)
	if err != nil {
		return nil, err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}
	if hostNetworkAddress != "" {
		host = hostNetworkAddress
	}

	envPrefix := strings.ToUpper(strings.Replace(clusterName, "-", "_", -1))
	if err := os.Setenv(fmt.Sprintf("%s_OPENSEARCH_HOST", envPrefix), host); err != nil {
		return nil, err
	}
	if err := os.Setenv(fmt.Sprintf("%s_OPENSEARCH_HTTP_PORT", envPrefix), exposedHTTPPort.Port()); err != nil {
		return nil, err
	}
	if err := os.Setenv(fmt.Sprintf("%s_OPENSEARCH_TCP_PORT", envPrefix), exposedTCPPort.Port()); err != nil {
		return nil, err
	}

	return &OpenSearchContainer{
		Container:   container,
		clusterName: clusterName,
		// DISABLE_SECURITY_PLUGIN=true is set, so empty user/password will do for test clients
		username:        "",
		password:        "",
		host:            host,
		exposedHTTPPort: exposedHTTPPort,
		exposedTCPPort:  exposedTCPPort,
	}, nil
}
