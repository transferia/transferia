//go:build !disable_mysql_provider

package mysqlrecipe

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/tests/tcrecipes"
)

const (
	rootUser        = "root"
	defaultUser     = "db_user"
	defaultPassword = "P@ssw0rd"
	defaultVersion  = "mysql/mysql-server:5.7.40"
	SourceDB        = "source"
	TargetDB        = "target"
)

// MySQLContainer represents the MySQL container type used in the module
type MySQLContainer struct {
	testcontainers.Container
	username string
	password string
	database string
}

func WithDefaultCredentials() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		username := req.Env["MYSQL_USER"]
		password := req.Env["MYSQL_PASSWORD"]
		if strings.EqualFold(rootUser, username) {
			delete(req.Env, "MYSQL_USER")
		}
		if len(password) != 0 && password != "" {
			req.Env["MYSQL_ROOT_PASSWORD"] = password
		} else if strings.EqualFold(rootUser, username) {
			req.Env["MYSQL_ALLOW_EMPTY_PASSWORD"] = "yes"
			delete(req.Env, "MYSQL_PASSWORD")
		}

		return nil
	}
}

func PrepareContainer(ctx context.Context) {
	if _, ok := os.LookupEnv("RECIPE_MYSQL_HOST"); ok {
		return // container already initialized
	}
	if tcrecipes.Enabled() {
		_, err := Run(ctx, defaultVersion)
		if err != nil {
			panic(err)
		}
	}
}

// Run creates an instance of the MySQL container type
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*MySQLContainer, error) {
	tz, _ := time.Now().Zone()

	req := testcontainers.ContainerRequest{
		Image: img,
		Env: map[string]string{
			"MYSQL_ALLOW_EMPTY_PASSWORD": "yes",
			"TZ":                         tz,
		},
		ExposedPorts: []string{"3306/tcp", "33060/tcp"},
		WaitingFor:   wait.ForLog("port: 3306  MySQL Community Server"),
		Cmd: []string{
			"mysqld",
			"--datadir=/var/lib/mysql",
			"--server-id=1",
			"--log-bin=/var/lib/mysql/mysql-bin.log",
		},
	}

	f, err := os.CreateTemp("", "mysql-init")
	if err != nil {
		return nil, xerrors.Errorf("unable to init temp file: %w", err)
	}

	defer f.Close()

	initSQL := fmt.Sprintf(`
CREATE DATABASE %[1]s;
CREATE DATABASE %[2]s;
CREATE USER '%[3]s'@'%%' IDENTIFIED BY '%[4]s';
GRANT ALL PRIVILEGES ON *.* TO '%[3]s'@'%%';
SET GLOBAL time_zone = "%[5]s";
`, SourceDB, TargetDB, defaultUser, defaultPassword, tz)
	if _, err := f.Write([]byte(initSQL)); err != nil {
		return nil, xerrors.Errorf("unable to write init script: %w", err)
	}

	req.Files = append(req.Files, testcontainers.ContainerFile{
		HostFilePath:      f.Name(),
		ContainerFilePath: "/docker-entrypoint-initdb.d/0000001_init_src_dst.sql",
		FileMode:          0o755,
	})

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	opts = append(opts, WithDefaultCredentials())

	for _, opt := range opts {
		if err := opt.Customize(&genericContainerReq); err != nil {
			return nil, xerrors.Errorf("unable to customize container: %w", err)
		}
	}

	username, ok := req.Env["MYSQL_USER"]
	if !ok {
		username = rootUser
	}
	password := req.Env["MYSQL_PASSWORD"]

	if len(password) == 0 && password == "" && !strings.EqualFold(rootUser, username) {
		return nil, xerrors.New("empty password can be used only with the root user")
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, xerrors.Errorf("generic container: %w", err)
	}
	var c *MySQLContainer
	if container != nil {
		containerPort, err := container.MappedPort(ctx, "3306/tcp")
		if err != nil {
			return nil, xerrors.Errorf("unable to get mysql port: %w", err)
		}
		if err := os.Setenv("RECIPE_MYSQL_PORT", fmt.Sprintf("%v", containerPort.Port())); err != nil {
			return nil, xerrors.Errorf("unable to set PG_LOCAL_PORT env: %w", err)
		}
		c = &MySQLContainer{
			Container: container,
			password:  password,
			username:  username,
			database:  SourceDB,
		}
	}

	if err := os.Setenv("RECIPE_MYSQL_HOST", "localhost"); err != nil {
		return nil, xerrors.Errorf("unable to set RECIPE_MYSQL_HOST env: %w", err)
	}
	if err := os.Setenv("RECIPE_MYSQL_USER", defaultUser); err != nil {
		return nil, xerrors.Errorf("unable to set RECIPE_MYSQL_USER env: %w", err)
	}
	if err := os.Setenv("RECIPE_MYSQL_PASSWORD", defaultPassword); err != nil {
		return nil, xerrors.Errorf("unable to set PG_LOCAL_DATABASE env: %w", err)
	}
	if err := os.Setenv("RECIPE_MYSQL_SOURCE_DATABASE", SourceDB); err != nil {
		return nil, xerrors.Errorf("unable to set PG_LOCAL_DATABASE env: %w", err)
	}
	if err := os.Setenv("RECIPE_MYSQL_TARGET_DATABASE", TargetDB); err != nil {
		return nil, xerrors.Errorf("unable to set PG_LOCAL_DATABASE env: %w", err)
	}
	if err := InitScripts(); err != nil {
		return nil, xerrors.Errorf("unable to apply init scripts: %w", err)
	}
	return c, nil
}

func InitScripts() error {
	knownSourceDumps := []string{
		"dump",
		"source",
		"src",
	}
	srcParams, err := mysql.NewConnectionParams(RecipeMysqlSource().ToStorageParams())
	if err != nil {
		return xerrors.Errorf("unable to build conn params: %w", err)
	}
	for _, dir := range knownSourceDumps {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if !os.IsExist(err) {
				continue
			}
			return xerrors.Errorf("unable to read dir: %w", err)
		}

		for _, e := range entries {
			data, err := os.ReadFile(dir + "/" + e.Name())
			if err != nil {
				return xerrors.Errorf("unable to read: %s: %w", e.Name(), err)
			}
			if err := Exec(string(data), srcParams); err != nil {
				return xerrors.Errorf("unable to exec query: %w", err)
			}
		}
	}
	return nil
}

func (c *MySQLContainer) MustSourceConnectionString(ctx context.Context, args ...string) string {
	addr, err := c.ConnectionString(ctx, SourceDB, args...)
	if err != nil {
		panic(err)
	}
	return addr
}

func (c *MySQLContainer) ConnectionString(ctx context.Context, db string, args ...string) (string, error) {
	containerPort, err := c.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return "", xerrors.Errorf("unable to get port: %w", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		return "", xerrors.Errorf("unable to get host: %w", err)
	}

	extraArgs := ""
	if len(args) > 0 {
		extraArgs = strings.Join(args, "&")
	}
	if extraArgs != "" {
		extraArgs = "?" + extraArgs
	}

	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s%s", c.username, c.password, host, containerPort.Port(), db, extraArgs)
	return connectionString, nil
}

func (c *MySQLContainer) MustTargetConnectionString(ctx context.Context, args ...string) string {
	addr, err := c.ConnectionString(ctx, TargetDB, args...)
	if err != nil {
		panic(err)
	}
	return addr
}
