package iceberg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	DefaultAccessKey = "admin"
	DefaultSecretKey = "password"
	DefaultRegion    = "us-east-1"
	DefaultBucket    = "warehouse"

	minioImage = "minio/minio"
	restImage  = "apache/iceberg-rest-fixture"

	minioPort        = "9000/tcp"
	minioConsolePort = "9001/tcp"
	restPort         = "8181/tcp"
)

// IcebergCluster encapsulates all containers required for Iceberg integration testing:
// MinIO (S3-compatible storage) and Iceberg REST Catalog.
type IcebergCluster struct {
	minioCont tc.Container
	restCont  tc.Container
}

// RunCluster creates and starts all containers that form the Iceberg test environment.
// On any error the already-started containers are terminated before returning.
func RunCluster(ctx context.Context) (_ *IcebergCluster, err error) {
	var cluster IcebergCluster

	defer func() {
		if err != nil {
			_ = cluster.Close(ctx)
		}
	}()

	// --- MinIO ---
	cluster.minioCont, err = tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ProviderType: tc.ProviderPodman,
		ContainerRequest: tc.ContainerRequest{
			Image:        minioImage,
			ExposedPorts: []string{minioPort, minioConsolePort},
			Env: map[string]string{
				"MINIO_ROOT_USER":     DefaultAccessKey,
				"MINIO_ROOT_PASSWORD": DefaultSecretKey,
				"MINIO_DOMAIN":        "minio",
			},
			Cmd:        []string{"server", "/data", "--console-address", ":9001"},
			WaitingFor: wait.ForListeningPort(minioPort).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start minio container: %w", err)
	}

	minioIP, err := cluster.minioCont.ContainerIP(ctx)
	if err != nil {
		return nil, fmt.Errorf("get minio container IP: %w", err)
	}

	minioEndpoint, err := containerURL(ctx, cluster.minioCont, minioPort)
	if err != nil {
		return nil, fmt.Errorf("get minio endpoint: %w", err)
	}
	if err := createBucket(ctx, minioEndpoint, DefaultAccessKey, DefaultSecretKey, DefaultRegion, DefaultBucket); err != nil {
		return nil, fmt.Errorf("create warehouse bucket: %w", err)
	}

	// --- Iceberg REST Catalog ---
	cluster.restCont, err = tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ProviderType: tc.ProviderPodman,
		ContainerRequest: tc.ContainerRequest{
			Image:        restImage,
			ExposedPorts: []string{restPort},
			ExtraHosts: []string{
				fmt.Sprintf("minio:%s", minioIP),
				fmt.Sprintf("%s.minio:%s", DefaultBucket, minioIP),
			},
			Env: map[string]string{
				"AWS_ACCESS_KEY_ID":     DefaultAccessKey,
				"AWS_SECRET_ACCESS_KEY": DefaultSecretKey,
				"AWS_REGION":            DefaultRegion,
				"CATALOG_WAREHOUSE":     "s3://" + DefaultBucket + "/",
				"CATALOG_IO__IMPL":      "org.apache.iceberg.aws.s3.S3FileIO",
				"CATALOG_S3_ENDPOINT":   "http://minio:9000",
			},
			WaitingFor: wait.ForListeningPort(restPort).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start iceberg rest container: %w", err)
	}

	return &cluster, nil
}

// MustRunCluster is a test helper that starts the Iceberg cluster and registers
// automatic cleanup via t.Cleanup. It fails the test immediately on error.
func MustRunCluster(t *testing.T, ctx context.Context) *IcebergCluster {
	t.Helper()

	cluster, err := RunCluster(ctx)
	require.NoError(t, err, "failed to start iceberg cluster")

	t.Cleanup(func() {
		if err := cluster.Close(ctx); err != nil {
			t.Logf("warning: iceberg cluster cleanup: %v", err)
		}
	})

	restURL, err := cluster.RESTCatalogURL(ctx)
	if err == nil {
		t.Logf("Iceberg REST catalog: %s", restURL)
	}

	minioURL, err := cluster.MinioEndpoint(ctx)
	if err == nil {
		t.Logf("MinIO S3 endpoint: %s", minioURL)
	}

	consoleURL, err := cluster.MinioConsoleURL(ctx)
	if err == nil {
		t.Logf("MinIO Console: %s", consoleURL)
	}

	return cluster
}

// RESTCatalogURL returns the external http://host:port address of the Iceberg REST catalog.
func (c *IcebergCluster) RESTCatalogURL(ctx context.Context) (string, error) {
	return containerURL(ctx, c.restCont, restPort)
}

// MinioEndpoint returns the external http://host:port address of the MinIO S3 API.
func (c *IcebergCluster) MinioEndpoint(ctx context.Context) (string, error) {
	return containerURL(ctx, c.minioCont, minioPort)
}

// MinioConsoleURL returns the external http://host:port address of the MinIO web console.
func (c *IcebergCluster) MinioConsoleURL(ctx context.Context) (string, error) {
	return containerURL(ctx, c.minioCont, minioConsolePort)
}

// Close terminates all containers in reverse startup order.
// It collects all errors and returns them joined.
func (c *IcebergCluster) Close(ctx context.Context) error {
	var errs []error
	for _, cont := range []tc.Container{c.restCont, c.minioCont} {
		if cont != nil {
			if err := cont.Terminate(ctx); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("terminate containers: %v", errs)
	}
	return nil
}

func containerURL(ctx context.Context, cont tc.Container, port string) (string, error) {
	host, err := cont.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("get host: %w", err)
	}
	mapped, err := cont.MappedPort(ctx, nat.Port(port))
	if err != nil {
		return "", fmt.Errorf("get mapped port: %w", err)
	}
	return fmt.Sprintf("http://%s:%s", host, mapped.Port()), nil
}

func createBucket(ctx context.Context, endpoint, accessKey, secretKey, region, bucket string) error {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return fmt.Errorf("create aws session: %w", err)
	}

	client := s3.New(sess)

	if _, err := client.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		return fmt.Errorf("create bucket %q: %w", bucket, err)
	}

	policy := fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],"Resource":["arn:aws:s3:::%s","arn:aws:s3:::%s/*"]}]}`,
		bucket, bucket,
	)
	if _, err := client.PutBucketPolicyWithContext(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(policy),
	}); err != nil {
		return fmt.Errorf("set bucket policy: %w", err)
	}

	return nil
}
