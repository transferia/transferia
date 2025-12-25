package azure

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	EventHubPort = "5672/tcp"
)

// EventHubContainer represents the Eventhub container type used in the module
type EventHubContainer struct {
	testcontainers.Container
	Settings options
}

func (c *EventHubContainer) ServiceURL(ctx context.Context) (string, error) {
	hostname, err := c.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("unable to get hostname: %w", err)
	}

	mappedPort, err := c.MappedPort(ctx, EventHubPort)
	if err != nil {
		return "", fmt.Errorf("unable to get mapped port: %s: %w", EventHubPort, err)
	}

	return fmt.Sprintf("http://%s", net.JoinHostPort(hostname, mappedPort.Port())), nil
}

func (c *EventHubContainer) MustServiceURL(ctx context.Context) string {
	url, err := c.ServiceURL(ctx)
	if err != nil {
		panic(err)
	}

	return url
}

// Run creates an instance of the Eventhub container type
func RunEventHub(ctx context.Context, img string, blobAddress string, opts ...testcontainers.ContainerCustomizer) (*EventHubContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        img,
		WaitingFor:   wait.ForListeningPort(EventHubPort).WithStartupTimeout(3 * time.Second),
		ExposedPorts: []string{EventHubPort},
		Env:          map[string]string{"ACCEPT_EULA": "Y", "BLOB_SERVER": blobAddress, "METADATA_SERVER": blobAddress},
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	err := WithEventHub(&genericContainerReq)
	if err != nil {
		return nil, fmt.Errorf("unable to write data to the temporary file: %w", err)
	}

	// 1. Gather all config options (defaults and then apply provided options)
	settings := defaultOptions()
	for _, opt := range opts {
		if err := opt.Customize(&genericContainerReq); err != nil {
			return nil, err
		}
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, err
	}

	return &EventHubContainer{Container: container, Settings: settings}, nil
}

func WithEventHub(req *testcontainers.GenericContainerRequest) error {
	f, err := os.CreateTemp("", "eventhub-tc-config-")
	if err != nil {
		return fmt.Errorf("unable to create temporary file %w", err)
	}

	defer f.Close()

	// write data to the temporary file
	data := []byte(`
{
  "UserConfig": {
    "NamespaceConfig": [
      {
        "Type": "EventHub",
        "Name": "emulatorNs1",
        "Entities": [
          {
            "Name": "eh1",
            "PartitionCount": "2",
            "ConsumerGroups": [
              {
                "Name": "cg1"
              }
            ]
          }
        ]
      }
    ],
    "LoggingConfig": {
      "Type": "File"
    }
  }
}`)
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("unable to write data to temporary file %w", err)
	}
	cf := testcontainers.ContainerFile{
		HostFilePath:      f.Name(),
		ContainerFilePath: "/Eventhubs_Emulator/ConfigFiles/Config.json",
		FileMode:          0755,
	}
	req.Files = append(req.Files, cf)

	return nil
}
