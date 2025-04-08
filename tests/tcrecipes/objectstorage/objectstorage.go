package objectstorage

import (
	"context"
	"fmt"
	"os"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const defaultImage = "quay.io/minio/minio"

const consolePort = nat.Port("9090/tcp")
const s3APIPort = nat.Port("9000/tcp")

type MiniIOContainer struct {
	testcontainers.Container
	exposedPort nat.Port
	consolePort nat.Port
}

func (c *MiniIOContainer) Port() nat.Port {
	return c.exposedPort
}

func (c *MiniIOContainer) ConsolePort() nat.Port {
	return c.consolePort
}

type Params struct {
	RootUser     string
	RootPassword string
}

func Prepare(ctx context.Context) (*MiniIOContainer, error) {
	params := Params{
		RootUser:     "root",
		RootPassword: "password",
	}
	cntr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			ExposedPorts: []string{consolePort.Port(), s3APIPort.Port()},
			Image:        defaultImage,
			WaitingFor:   wait.ForListeningPort(s3APIPort),
			Env: map[string]string{
				"MINIO_ROOT_USER":     params.RootUser,
				"MINIO_ROOT_PASSWORD": params.RootPassword,
			},
			Cmd: []string{
				"server",
				"--address", fmt.Sprintf(":%s", s3APIPort.Port()),
				"--console-address", fmt.Sprintf(":%s", consolePort.Port()),
				"/data",
			},
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}

	exposedPort, err := cntr.MappedPort(ctx, s3APIPort)
	if err != nil {
		return nil, err
	}

	consolePort, err := cntr.MappedPort(ctx, consolePort)
	if err != nil {
		return nil, err
	}

	if err := os.Setenv("S3_ACCESS_KEY", params.RootUser); err != nil {
		return nil, err
	}
	if err := os.Setenv("S3_SECRET", params.RootPassword); err != nil {
		return nil, err
	}
	if err := os.Setenv("S3MDS_PORT", exposedPort.Port()); err != nil {
		return nil, err
	}
	if err := os.Setenv("S3_REGION", "us-east-2"); err != nil {
		return nil, err
	}
	if err := os.Setenv("S3_ENDPOINT", fmt.Sprintf("http://localhost:%s", exposedPort.Port())); err != nil {
		return nil, err
	}

	return &MiniIOContainer{Container: cntr, exposedPort: exposedPort, consolePort: consolePort}, nil
}
