//go:build !disable_postgres_provider

package pgrecipe

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/tcrecipes"
	tc_postgres "github.com/transferia/transferia/tests/tcrecipes/postgres"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	selectCopyCommandsQuery = `
		SELECT
			'copy (select * from '||r.relname||' order by '|| array_to_string(array_agg(distinct a.attname order by a.attname), ',')|| ') to STDOUT;'
		FROM
			pg_class       r,
			pg_constraint  c,
			pg_attribute   a,
			pg_namespace   n
		WHERE 1=1
			AND r.oid = c.conrelid
			AND r.oid = a.attrelid
			AND a.attnum = ANY(c.conkey)
			AND c.contype = 'p'
			AND r.relkind = 'r'
			AND r.relname not like '__consumer_keeper'
			AND r.relnamespace = n.oid
			AND (n.nspname || '.' || r.relname) LIKE COALESCE(NULLIF($1, ''), '%')
		GROUP BY
			r.relname
		ORDER BY
			r.relname
		;`
)

type ContainerParams struct {
	prefix      string
	initScripts []string
	noPgDump    bool
	err         error
}

type ContainerOption func(opt *ContainerParams)

func withFiles(files ...string) ContainerOption {
	return func(opt *ContainerParams) {
		opt.initScripts = append(opt.initScripts, files...)
	}
}

func withInitDir(dir string) ContainerOption {
	return func(opt *ContainerParams) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			opt.err = err
			return
		}

		for _, e := range entries {
			opt.initScripts = append(opt.initScripts, dir+"/"+e.Name())
		}
	}
}

func withPrefix(prefix string) ContainerOption {
	return func(opt *ContainerParams) {
		opt.prefix = prefix
	}
}

type RecipeParams struct {
	endpointOptions  []func(pg *postgres.PgSource)
	containerOptions []ContainerOption
	prefix           string
	connectionID     string
}

func WithFiles(files ...string) RecipeOption {
	return func(pg *RecipeParams) {
		pg.containerOptions = append(pg.containerOptions, withFiles(files...))
	}
}

func WithInitDir(dir string) RecipeOption {
	return func(pg *RecipeParams) {
		pg.containerOptions = append(pg.containerOptions, withInitDir(dir))
	}
}

func WithPrefix(prefix string) RecipeOption {
	return func(pg *RecipeParams) {
		pg.prefix = prefix
	}
}

func WithInitFiles(files ...string) RecipeOption {
	return func(opt *RecipeParams) {
		opt.containerOptions = append(opt.containerOptions, withFiles(files...))
	}
}

func WithEdit(f func(pg *postgres.PgSource)) RecipeOption {
	return func(opt *RecipeParams) {
		opt.endpointOptions = append(opt.endpointOptions, f)
	}
}

func WithConnection(connID string) RecipeOption {
	return func(pg *RecipeParams) {
		pg.connectionID = connID
	}
}

func WithDBTables(tables ...string) RecipeOption {
	return WithEdit(func(pg *postgres.PgSource) {
		pg.DBTables = tables
	})
}

func WithoutPgDump() RecipeOption {
	return func(pg *RecipeParams) {
		pg.containerOptions = append(pg.containerOptions, func(opt *ContainerParams) {
			opt.noPgDump = true
		})
	}
}

type RecipeOption func(pg *RecipeParams)

// no need to recreate same postgres recipe prefix over and over again
var postgresContainers = map[string]bool{}

func ManagedConnection(opts ...RecipeOption) *connection.ConnectionPG {
	params := new(RecipeParams)
	params.prefix = "SOURCE_"
	for _, f := range opts {
		f(params)
	}

	port, _ := strconv.Atoi(os.Getenv(params.prefix + "PG_LOCAL_PORT"))

	return &connection.ConnectionPG{
		BaseSQLConnection: &connection.BaseSQLConnection{
			Hosts:          []*connection.Host{{Name: "localhost", Port: port, Role: connection.RoleUnknown, ReplicaType: connection.ReplicaUndefined}},
			User:           os.Getenv(params.prefix + "PG_LOCAL_USER"),
			Password:       model.SecretString(os.Getenv(params.prefix + "PG_LOCAL_PASSWORD")),
			Database:       os.Getenv(params.prefix + "PG_LOCAL_DATABASE"),
			HasTLS:         false,
			CACertificates: "",
			ClusterID:      "",
		},
		DatabaseNames: nil,
	}
}

func RecipeSource(opts ...RecipeOption) *postgres.PgSource {
	params := new(RecipeParams)
	params.prefix = "SOURCE_"
	for _, f := range opts {
		f(params)
	}
	if tcrecipes.Enabled() {
		params.containerOptions = append(params.containerOptions, withPrefix(params.prefix))
		_, err := PostgresContainer(params.containerOptions...)
		if err != nil {
			panic(err)
		}
	}

	v := new(postgres.PgSource)

	if params.connectionID == "" {
		srcPort, _ := strconv.Atoi(os.Getenv(params.prefix + "PG_LOCAL_PORT"))
		v.Hosts = []string{"localhost"}
		v.User = os.Getenv(params.prefix + "PG_LOCAL_USER")
		v.Password = model.SecretString(os.Getenv(params.prefix + "PG_LOCAL_PASSWORD"))
		v.Port = srcPort
	} else {
		v.ConnectionID = params.connectionID
	}

	v.Database = os.Getenv(params.prefix + "PG_LOCAL_DATABASE")
	v.PgDumpCommand = []string{os.Getenv(params.prefix+"PG_LOCAL_BIN_PATH") + "/pg_dump"}
	v.SlotID = "testslot"
	v.BatchSize = 10
	v.WithDefaults()
	for _, f := range params.endpointOptions {
		f(v)
	}
	return v
}

func RecipeTarget(opts ...RecipeOption) *postgres.PgDestination {
	params := new(RecipeParams)
	params.prefix = "TARGET_"
	for _, f := range opts {
		f(params)
	}
	if tcrecipes.Enabled() {
		params.containerOptions = append(params.containerOptions, withPrefix(params.prefix))
		_, err := PostgresContainer(params.containerOptions...)
		if err != nil {
			panic(err)
		}
	}

	v := new(postgres.PgDestination)
	if params.connectionID == "" {
		dstPort, _ := strconv.Atoi(os.Getenv(params.prefix + "PG_LOCAL_PORT"))
		v.Hosts = []string{"localhost"}
		v.User = os.Getenv(params.prefix + "PG_LOCAL_USER")
		v.Password = model.SecretString(os.Getenv(params.prefix + "PG_LOCAL_PASSWORD"))
		v.Port = dstPort
	} else {
		v.ConnectionID = params.connectionID
	}

	v.Database = os.Getenv(params.prefix + "PG_LOCAL_DATABASE")
	v.WithDefaults()
	return v
}

func PostgresContainer(opts ...ContainerOption) (*tc_postgres.PostgresContainer, error) {
	runOpts := new(ContainerParams)
	for _, opt := range opts {
		opt(runOpts)
	}

	if postgresContainers[runOpts.prefix] {
		return new(tc_postgres.PostgresContainer), nil
	}
	postgresContainers[runOpts.prefix] = true
	if runOpts.err != nil {
		return nil, xerrors.Errorf("unable to prepare recipe options: %w", runOpts.err)
	}
	pgc, err := tc_postgres.Prepare(
		context.Background(),
		tc_postgres.WithConfigFile(`
log_min_error_statement = fatal
listen_addresses = '*'
shared_preload_libraries = 'decoderbufs'
wal_level = logical             # minimal, archive, hot_standby, or logical (change requires restart)
max_wal_senders = 4             # max number of walsender processes (change requires restart)
max_replication_slots = 64       # max number of replication slots (change requires restart)
`),
		tc_postgres.WithDatabase("postgres"),
		tc_postgres.WithUsername("postgres"),
		tc_postgres.WithPassword("123"),
		tc_postgres.WithImage("debezium/postgres:11-alpine"),
		tc_postgres.WithInitScripts(runOpts.initScripts...),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to prepare postgres: %w", err)
	}

	if err := os.Setenv(runOpts.prefix+"PG_LOCAL_USER", "postgres"); err != nil {
		return nil, xerrors.Errorf("unable to set PG_LOCAL_USER env: %w", err)
	}
	if err := os.Setenv(runOpts.prefix+"PG_LOCAL_PASSWORD", "123"); err != nil {
		return nil, xerrors.Errorf("unable to set PG_LOCAL_PASSWORD env: %w", err)
	}
	if err := os.Setenv(runOpts.prefix+"PG_LOCAL_DATABASE", "postgres"); err != nil {
		return nil, xerrors.Errorf("unable to set PG_LOCAL_DATABASE env: %w", err)
	}
	if err := os.Setenv(runOpts.prefix+"PG_LOCAL_PORT", fmt.Sprintf("%v", pgc.Port())); err != nil {
		return nil, xerrors.Errorf("unable to set PG_LOCAL_PORT env: %w", err)
	}
	if !runOpts.noPgDump {
		pgdumpPath, err := exec.LookPath("pg_dump")
		if err != nil {
			return nil, xerrors.Errorf("unable to locate local pgdump: %w", err)
		}
		if err := os.Setenv(runOpts.prefix+"PG_LOCAL_BIN_PATH", filepath.Dir(pgdumpPath)); err != nil {
			return nil, xerrors.Errorf("unable to set PG_LOCAL_BIN_PATH env: %w", err)
		}
	}
	return pgc, nil
}

// PgDump dumps all tables in the PostgreSQL database specified in the connString.
// Use the specified pgDumpExecutable for dumping schemas, and the given
// psqlExecutable for dumping table data.
// You can specify tableLikePattern to filter tables in the dump, for example, 'public.my%table'
func PgDump(t *testing.T, pgDumpExecutable []string, psqlExecutable []string, connString string, tableLikePattern string) string {
	pgDumpArgs := append(pgDumpExecutable, "--no-publications", "--no-subscriptions", "--format=plain", "--no-owner", "--schema-only", connString)
	pgDumpCommand := exec.Command(pgDumpArgs[0], pgDumpArgs[1:]...)

	var pgDumpStdout, pgDumpStderr bytes.Buffer
	pgDumpCommand.Stdout = &pgDumpStdout
	pgDumpCommand.Stderr = &pgDumpStderr
	logger.Log.Info("Run pg_dump", log.String("path", pgDumpCommand.Path), log.Strings("args", pgDumpCommand.Args))
	require.NoError(t, pgDumpCommand.Run(), pgDumpStderr.String())

	conn, err := pgx.Connect(context.Background(), connString)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	queryRows, err := conn.Query(context.Background(), selectCopyCommandsQuery, tableLikePattern)
	require.NoError(t, err)
	defer queryRows.Close()
	var copyCommands []string
	for queryRows.Next() {
		var query string
		require.NoError(t, queryRows.Scan(&query))
		copyCommands = append(copyCommands, query)
	}

	var dump strings.Builder
	dump.Write(pgDumpStdout.Bytes())

	for _, copyCommand := range copyCommands {
		psqlArgs := append(psqlExecutable, connString, "-c", copyCommand)
		psqlCmd := exec.Command(psqlArgs[0], psqlArgs[1:]...)
		var psqlStdout, psqlStderr, tableDump bytes.Buffer
		psqlCmd.Stdout = &psqlStdout
		psqlCmd.Stderr = &psqlStderr
		psqlCmd.Stdin = bytes.NewReader([]byte(copyCommand))
		logger.Log.Info("Run psql", log.String("path", psqlCmd.Path), log.Strings("args", psqlCmd.Args))
		require.NoError(t, psqlCmd.Run(), psqlStderr)
		tableDump.WriteString(copyCommand + "\n")
		tableDump.Write(psqlStdout.Bytes())
		tableDump.WriteString("\n\n")

		dump.Write(tableDump.Bytes())
	}

	return dump.String()
}

func PgDropDatabase(t *testing.T, psqlExecutable []string, connString string, database string) {
	const dropDatabase = `drop database if exists %s with (force) ;`
	sqlQuery := fmt.Sprintf(dropDatabase, database)
	psqlArgs := append(psqlExecutable, connString, "-c", sqlQuery)
	psqlCmd := exec.Command(psqlArgs[0], psqlArgs[1:]...)
	var psqlStdout, psqlStderr bytes.Buffer
	psqlCmd.Stdout = &psqlStdout
	psqlCmd.Stderr = &psqlStderr
	psqlCmd.Stdin = bytes.NewReader([]byte(sqlQuery))
	logger.Log.Info("Run psql", log.String("path", psqlCmd.Path), log.Strings("args", psqlCmd.Args))
	require.NoError(t, psqlCmd.Run(), psqlStderr.String())
}

func PgCreateDatabase(t *testing.T, psqlExecutable []string, connString string, database, owner string) {
	const createDB = `create database %s with owner %s;`
	sqlQuery := fmt.Sprintf(createDB, database, owner)
	psqlArgs := append(psqlExecutable, connString, "-c", sqlQuery)
	psqlCmd := exec.Command(psqlArgs[0], psqlArgs[1:]...)
	var psqlStdout, psqlStderr bytes.Buffer
	psqlCmd.Stdout = &psqlStdout
	psqlCmd.Stderr = &psqlStderr
	psqlCmd.Stdin = bytes.NewReader([]byte(sqlQuery))
	logger.Log.Info("Run psql", log.String("path", psqlCmd.Path), log.Strings("args", psqlCmd.Args))
	require.NoError(t, psqlCmd.Run(), psqlStderr.String())
}
