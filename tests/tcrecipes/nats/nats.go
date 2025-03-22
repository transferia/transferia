package nats

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"time"
)

const (
	natsImage   = "nats:latest"
	clientPort  = "4222/tcp"
	natsTestUrl = "NATS_TEST_URL"
)

// NATSContainer represents the running NATS container
type NATSContainer struct {
	testcontainers.Container
}

// WithStreams adds a lifecycle hook to create a JetStream stream after the container starts
func WithStreams(streamConfigs []*nats.StreamConfig) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if streamConfigs == nil || len(streamConfigs) == 0 {
			return nil
		}

		req.ContainerRequest.LifecycleHooks = append(req.ContainerRequest.LifecycleHooks, testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{
				func(ctx context.Context, c testcontainers.Container) error {
					fmt.Println("Waiting for NATS JetStream to be ready...")

					// Get the mapped NATS port
					host, err := c.Host(ctx)
					if err != nil {
						return err
					}

					port, err := c.MappedPort(ctx, clientPort)
					if err != nil {
						return err
					}

					natsURL := fmt.Sprintf("nats://%s:%s", host, port.Port())

					time.Sleep(5 * time.Second)

					// Connect to NATS
					nc, err := nats.Connect(natsURL)
					if err != nil {
						return fmt.Errorf("failed to connect to NATS: %w", err)
					}
					defer nc.Close()
					// Get JetStream Context
					js, err := nc.JetStream()
					if err != nil {
						return fmt.Errorf("failed to get JetStream context: %w", err)
					}

					// Create each stream
					for _, streamConfig := range streamConfigs {
						fmt.Printf("Creating stream: %s\n", streamConfig.Name)
						_, err := js.AddStream(streamConfig)
						if err != nil {
							return fmt.Errorf("failed to create stream '%s': %w", streamConfig.Name, err)
						}
						fmt.Printf("Stream '%s' created successfully\n", streamConfig.Name)
					}

					return nil
				},
			},
		})

		return nil
	}
}

// RunContainer starts a NATS container programmatically
func RunContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*NATSContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        natsImage,
		ExposedPorts: []string{clientPort},
		Env: map[string]string{
			"NATS_SERVER_NAME": "test-nats",
		},
		Cmd:        []string{"-js", "-DV"}, // Enable JetStream
		WaitingFor: wait.ForLog("Server is ready").WithStartupTimeout(100 * time.Second),
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

	host, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := container.MappedPort(ctx, clientPort)
	if err != nil {
		return nil, err
	}
	natsURL := fmt.Sprintf("nats://%s:%s", host, port.Port())
	// saving test url in env variable since we dont have access to containers outside.
	err = os.Setenv(natsTestUrl, natsURL)
	if err != nil {
		return nil, err
	}
	fmt.Println(os.Getenv(natsTestUrl))

	return &NATSContainer{Container: container}, nil
}
