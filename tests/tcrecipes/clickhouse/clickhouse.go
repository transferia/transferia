package clickhouse

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const defaultUser = "default"
const defaultDatabaseName = "clickhouse"

const defaultImage = "clickhouse/clickhouse-server:23.3.8.21-alpine"

const HTTPPort = nat.Port("8123/tcp")
const NativePort = nat.Port("9000/tcp")

// ClickHouseContainer represents the ClickHouse container type used in the module
type ClickHouseContainer struct {
	testcontainers.Container
	dbName   string
	user     string
	password string
}

func (c *ClickHouseContainer) ConnectionHost(ctx context.Context) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}

	port, err := c.MappedPort(ctx, NativePort)
	if err != nil {
		return "", err
	}

	return host + ":" + port.Port(), nil
}

// ConnectionString returns the dsn string for the clickhouse container, using the default 9000 port, and
// obtaining the host and exposed port from the container. It also accepts a variadic list of extra arguments
// which will be appended to the dsn string. The format of the extra arguments is the same as the
// connection string format, e.g. "dial_timeout=300ms" or "skip_verify=false"
func (c *ClickHouseContainer) ConnectionString(ctx context.Context, args ...string) (string, error) {
	host, err := c.ConnectionHost(ctx)
	if err != nil {
		return "", err
	}

	extraArgs := ""
	if len(args) > 0 {
		extraArgs = strings.Join(args, "&")
	}
	if extraArgs != "" {
		extraArgs = "?" + extraArgs
	}

	connectionString := fmt.Sprintf("clickhouse://%s:%s@%s/%s%s", c.user, c.password, host, c.dbName, extraArgs)
	return connectionString, nil
}

// WithInitScripts sets the init scripts to be run when the container starts
func WithInitScripts(scripts ...string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		initScripts := []testcontainers.ContainerFile{}
		for _, script := range scripts {
			cf := testcontainers.ContainerFile{
				HostFilePath:      script,
				ContainerFilePath: "/docker-entrypoint-initdb.d/" + filepath.Base(script),
				FileMode:          0755,
			}
			initScripts = append(initScripts, cf)
		}
		req.Files = append(req.Files, initScripts...)

		return nil
	}
}

// WithConfigFile sets the XML config file to be used for the clickhouse container
// It will also set the "configFile" parameter to the path of the config file
// as a command line argument to the container.
func WithConfigFile(configFile string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		name := path.Base(configFile)
		cf := testcontainers.ContainerFile{
			HostFilePath:      configFile,
			ContainerFilePath: path.Join("/etc/clickhouse-server/config.d/", name),
			FileMode:          0755,
		}
		req.Files = append(req.Files, cf)

		return nil
	}
}

func WithConfigData(data string) testcontainers.CustomizeRequestOption {
	f, err := os.CreateTemp("", "clickhouse-tc-config-*.xml") // in Go version older than 1.17 you can use ioutil.TempFile
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// write data to the temporary file
	if _, err := f.Write([]byte(data)); err != nil {
		panic(err)
	}

	return WithConfigFile(f.Name())
}

// WithConfigFile sets the YAML config file to be used for the clickhouse container
// It will also set the "configFile" parameter to the path of the config file
// as a command line argument to the container.
func WithYamlConfigFile(configFile string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		cf := testcontainers.ContainerFile{
			HostFilePath:      configFile,
			ContainerFilePath: "/etc/clickhouse-server/config.d/config.yaml",
			FileMode:          0755,
		}
		req.Files = append(req.Files, cf)

		return nil
	}
}

// WithDatabase sets the initial database to be created when the container starts
// It can be used to define a different name for the default database that is created when the image is first started.
// If it is not specified, then the default value("clickhouse") will be used.
func WithDatabase(dbName string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["CLICKHOUSE_DB"] = dbName

		return nil
	}
}

// WithPassword sets the initial password of the user to be created when the container starts
// It is required for you to use the ClickHouse image. It must not be empty or undefined.
// This environment variable sets the password for ClickHouse.
func WithPassword(password string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["CLICKHOUSE_PASSWORD"] = password

		return nil
	}
}

// WithUsername sets the initial username to be created when the container starts
// It is used in conjunction with WithPassword to set a user and its password.
// It will create the specified user with superuser power.
// If it is not specified, then the default user of clickhouse will be used.
func WithUsername(user string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if user == "" {
			user = defaultUser
		}

		req.Env["CLICKHOUSE_USER"] = user

		return nil
	}
}

func WithZookeeper(container *ZookeeperContainer) testcontainers.CustomizeRequestOption {
	return WithConfigData(fmt.Sprintf(`<?xml version="1.0"?>
	<clickhouse>
		<logger>
			<level>debug</level>
			<console>true</console>
			<log remove="remove"/>
			<errorlog remove="remove"/>
		</logger>

		<query_log>
			<database>system</database>
			<table>query_log</table>
		</query_log>

		<timezone>Europe/Berlin</timezone>

		<zookeeper>
			<node index="1">
				<host>%s</host>
				<port>2181</port>
			</node>
		</zookeeper>

		<remote_servers>
			<default>
				<shard>
					<replica>
						<host>localhost</host>
						<port>9000</port>
					</replica>
				</shard>
			</default>
		</remote_servers>
		<macros>
			<cluster>default</cluster>
			<shard>shard</shard>
			<replica>replica</replica>
		</macros>

		<distributed_ddl>
			<path>/clickhouse/task_queue/ddl</path>
		</distributed_ddl>
		<keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
		<format_schema_path>/var/lib/clickhouse/format_schemas/</format_schema_path>
	</clickhouse>
	`, container.IP()))
}

// Prepare creates an instance of the ClickHouse container type
func Prepare(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*ClickHouseContainer, error) {
	req := testcontainers.ContainerRequest{
		Image: defaultImage,
		Env: map[string]string{
			"CLICKHOUSE_USER":     defaultUser,
			"CLICKHOUSE_PASSWORD": defaultUser,
			"CLICKHOUSE_DB":       defaultDatabaseName,
		},
		ExposedPorts: []string{HTTPPort.Port(), NativePort.Port()},
		WaitingFor: wait.ForAll(
			wait.NewHostPortStrategy(NativePort),
			wait.NewHTTPStrategy("/").WithPort(HTTPPort).WithStatusCodeMatcher(func(status int) bool {
				return status == 200
			}).WithStartupTimeout(10*time.Second),
		),
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

	user := req.Env["CLICKHOUSE_USER"]
	password := req.Env["CLICKHOUSE_PASSWORD"]
	dbName := req.Env["CLICKHOUSE_DB"]

	nativePortExposed, err := container.MappedPort(ctx, NativePort)
	if err != nil {
		return nil, err
	}

	if err := os.Setenv("RECIPE_CLICKHOUSE_NATIVE_PORT", nativePortExposed.Port()); err != nil {
		return nil, err
	}

	if err := os.Setenv("RECIPE_CLICKHOUSE_PASSWORD", req.Env["CLICKHOUSE_PASSWORD"]); err != nil {
		return nil, err
	}

	return &ClickHouseContainer{Container: container, dbName: dbName, password: password, user: user}, nil
}
