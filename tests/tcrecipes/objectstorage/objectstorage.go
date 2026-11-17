package objectstorage

import (
	"context"
	"fmt"
	"os"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// MinIO images are no longer published, LocalStack serves the S3 API instead.
const defaultImage = "localstack/localstack:4.0.3"

const s3APIPort = nat.Port("4566/tcp")

type MiniIOContainer struct {
	testcontainers.Container
	exposedPort nat.Port
}

func (c *MiniIOContainer) Port() nat.Port {
	return c.exposedPort
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
			ExposedPorts: []string{s3APIPort.Port()},
			Image:        defaultImage,
			WaitingFor:   wait.ForHTTP("/_localstack/health").WithPort(s3APIPort),
			Env:          map[string]string{"SERVICES": "s3"},
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

	if err := os.Setenv("S3_ACCESS_KEY", params.RootUser); err != nil {
		return nil, err
	}
	if err := os.Setenv("S3_SECRET", params.RootPassword); err != nil {
		return nil, err
	}
	if err := os.Setenv("S3MDS_PORT", exposedPort.Port()); err != nil {
		return nil, err
	}
	if err := os.Setenv("S3_REGION", "us-east-1"); err != nil {
		return nil, err
	}
	if err := os.Setenv("S3_ENDPOINT", fmt.Sprintf("http://localhost:%s", exposedPort.Port())); err != nil {
		return nil, err
	}

	return &MiniIOContainer{Container: cntr, exposedPort: exposedPort}, nil
}
