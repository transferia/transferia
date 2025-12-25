package postgres

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/transferia/transferia/library/go/slices"
)

const defaultUser = "postgres"
const defaultPassword = "postgres"
const defaultPostgresImage = "docker.io/postgres:16-alpine"
const defaultPort = nat.Port("5432/tcp")

// PostgresContainer represents the postgres container type used in the module
type PostgresContainer struct {
	testcontainers.Container
	dbName      string
	user        string
	password    string
	exposedPort nat.Port
}

// ConnectionString returns the connection string for the postgres container, using the default 5432 port, and
// obtaining the host and exposed port from the container. It also accepts a variadic list of extra arguments
// which will be appended to the connection string. The format of the extra arguments is the same as the
// connection string format, e.g. "connect_timeout=10" or "application_name=myapp"
func (c *PostgresContainer) ConnectionString(ctx context.Context, args ...string) (string, error) {
	containerPort, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}

	extraArgs := strings.Join(args, "&")
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?%s", c.user, c.password, net.JoinHostPort(host, containerPort.Port()), c.dbName, extraArgs)
	return connStr, nil
}

func (c *PostgresContainer) Port() int {
	p, _ := strconv.Atoi(c.exposedPort.Port())
	return p
}

func WithConfigFile(cfg string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		f, err := os.CreateTemp("", "postgresql-conf-")
		if err != nil {
			panic(err)
		}

		defer f.Close()

		if _, err := f.Write([]byte(cfg)); err != nil {
			panic(err)
		}
		cfgFile := testcontainers.ContainerFile{
			HostFilePath:      f.Name(),
			ContainerFilePath: "/etc/postgresql.conf",
			FileMode:          0755,
		}

		req.Files = append(req.Files, cfgFile)
		req.Cmd = append(req.Cmd, "-c", "config_file=/etc/postgresql.conf")

		return nil
	}
}

// WithDatabase sets the initial database to be created when the container starts
// It can be used to define a different name for the default database that is created when the image is first started.
// If it is not specified, then the value of WithUser will be used.
func WithDatabase(dbName string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["POSTGRES_DB"] = dbName

		return nil
	}
}

func WithInitDir(dir string) testcontainers.CustomizeRequestOption {
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	return WithInitScripts(slices.Map(entries, func(t os.DirEntry) string {
		return dir + "/" + t.Name()
	})...)
}

// WithInitScripts sets the init scripts to be run when the container starts
func WithInitScripts(scripts ...string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		initScripts := []testcontainers.ContainerFile{}
		for _, script := range scripts {
			cf := testcontainers.ContainerFile{
				HostFilePath:      script,
				ContainerFilePath: fmt.Sprintf("/docker-entrypoint-initdb.d/%s", filepath.Base(script)),
				FileMode:          0755,
			}
			initScripts = append(initScripts, cf)
		}
		req.Files = append(req.Files, initScripts...)

		return nil
	}
}

// WithInitScripts sets the init scripts to be run when the container starts
func WithInitScript(script string, containerName string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		initScripts := []testcontainers.ContainerFile{}
		cf := testcontainers.ContainerFile{
			HostFilePath:      script,
			ContainerFilePath: fmt.Sprintf("/docker-entrypoint-initdb.d/%s", containerName),
			FileMode:          0755,
		}
		initScripts = append(initScripts, cf)
		req.Files = append(req.Files, initScripts...)

		return nil
	}
}

// WithPassword sets the initial password of the user to be created when the container starts
// It is required for you to use the PostgreSQL image. It must not be empty or undefined.
// This environment variable sets the superuser password for PostgreSQL.
func WithPassword(password string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["POSTGRES_PASSWORD"] = password
		if password == "" {
			req.Env["POSTGRES_HOST_AUTH_METHOD"] = "trust"
		}

		return nil
	}
}

// WithUsername sets the initial username to be created when the container starts
// It is used in conjunction with WithPassword to set a user and its password.
// It will create the specified user with superuser power and a database with the same name.
// If it is not specified, then the default user of postgres will be used.
func WithUsername(user string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if user == "" {
			user = defaultUser
		}

		req.Env["POSTGRES_USER"] = user

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

func WithImage(image string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if image == "" {
			image = defaultPostgresImage
		}

		req.Image = image

		return nil
	}
}

// Prepare creates an instance of the postgres container type
func Prepare(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*PostgresContainer, error) {
	req := testcontainers.ContainerRequest{
		Image: defaultPostgresImage,
		Env: map[string]string{
			"POSTGRES_USER":     defaultUser,
			"POSTGRES_PASSWORD": defaultPassword,
			"POSTGRES_DB":       defaultUser, // defaults to the user name
		},
		ExposedPorts: []string{defaultPort.Port()},
		Cmd:          []string{"postgres", "-c", "fsync=off"},
		WaitingFor:   wait.ForAll(wait.NewHostPortStrategy(defaultPort)),
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	for _, opt := range opts {
		_ = opt.Customize(&genericContainerReq)
	}
	if req.FromDockerfile.Dockerfile != "" {
		req.Image = ""
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, err
	}

	user := req.Env["POSTGRES_USER"]
	password := req.Env["POSTGRES_PASSWORD"]
	dbName := req.Env["POSTGRES_DB"]
	if err := os.Setenv(fmt.Sprintf("%s_POSTGRESQL_RECIPE_HOST", strings.ToUpper(dbName)), "localhost"); err != nil {
		return nil, err
	}
	exposedPort, err := container.MappedPort(ctx, defaultPort)
	if err != nil {
		return nil, err
	}
	if err := os.Setenv(fmt.Sprintf("%s_POSTGRESQL_RECIPE_PORT", strings.ToUpper(dbName)), exposedPort.Port()); err != nil {
		return nil, err
	}

	return &PostgresContainer{Container: container, dbName: dbName, password: password, user: user, exposedPort: exposedPort}, nil
}
