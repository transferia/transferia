package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

// ddlApplyTargetDB is a separate database on the recipe server. The schema is extracted by pg_dump
// from the recipe database and applied here, so CREATE statements do not hit "already exists".
const ddlApplyTargetDB = "ddl_apply_target"

type ddlApplyEnv struct {
	src *provider_postgres.PgSource
	dst *provider_postgres.PgDestination

	srcConn *pgxpool.Pool
	dstConn *pgxpool.Pool
}

func newDDLApplyEnv(t *testing.T, tables ...string) *ddlApplyEnv {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithDBTables(tables...))
	dst := pgrecipe.RecipeTarget(pgrecipe.WithPrefix(""))

	srcStorage, err := provider_postgres.NewStorage(dst.ToStorageParams())
	require.NoError(t, err)
	t.Cleanup(srcStorage.Close)
	ensureDatabase(t, srcStorage.Conn, ddlApplyTargetDB)

	dst.Database = ddlApplyTargetDB
	dstStorage, err := provider_postgres.NewStorage(dst.ToStorageParams())
	require.NoError(t, err)
	t.Cleanup(dstStorage.Close)

	return &ddlApplyEnv{
		src:     src,
		dst:     dst,
		srcConn: srcStorage.Conn,
		dstConn: dstStorage.Conn,
	}
}

func ensureDatabase(t *testing.T, conn *pgxpool.Pool, name string) {
	ctx := context.Background()
	var exists bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name).Scan(&exists))
	if exists {
		return
	}
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", name))
	require.NoError(t, err)
}

func execAll(t *testing.T, conn *pgxpool.Pool, queries ...string) {
	for _, query := range queries {
		_, err := conn.Exec(context.Background(), query)
		require.NoError(t, err, query)
	}
}

func (e *ddlApplyEnv) execSrc(t *testing.T, queries ...string) {
	execAll(t, e.srcConn, queries...)
}

func (e *ddlApplyEnv) execDst(t *testing.T, queries ...string) {
	execAll(t, e.dstConn, queries...)
}

// apply extracts the schema of the source tables by pg_dump and applies items of the given types on the target.
func (e *ddlApplyEnv) apply(t *testing.T, types ...provider_postgres.PgObjectType) error {
	transfer := helpers.MakeTransfer(helpers.TransferID, e.src, e.dst, abstract.TransferTypeSnapshotOnly)
	items, err := provider_postgres.ExtractPgDumpSchema(transfer)
	require.NoError(t, err)

	// pg_dump covers the whole shared recipe database, including service tables (e.g. public.__consumer_keeper)
	// that do not exist in the dedicated target database; keep only the objects created by this test.
	testItems := items[:0]
	for _, item := range items {
		if strings.HasPrefix(item.Schema, "ddl_") {
			testItems = append(testItems, item)
		}
	}
	require.NotEmpty(t, testItems)

	typeNames := make([]string, 0, len(types))
	for _, typ := range types {
		typeNames = append(typeNames, string(typ))
	}
	return provider_postgres.ApplyCommands(testItems, *transfer, &model.TransferOperation{}, helpers.EmptyRegistry(), typeNames...)
}

// setLockTimeout makes every new session to the target database fail fast on lock waits (SQLSTATE 55P03).
func (e *ddlApplyEnv) setLockTimeout(t *testing.T, timeout string) {
	e.execDst(t, fmt.Sprintf("ALTER DATABASE %s SET lock_timeout = '%s'", ddlApplyTargetDB, timeout))
	t.Cleanup(func() {
		_, _ = e.dstConn.Exec(context.Background(), fmt.Sprintf("ALTER DATABASE %s RESET lock_timeout", ddlApplyTargetDB))
	})
}

// lockTable takes ACCESS EXCLUSIVE lock on the target table in a separate transaction and returns a function releasing it.
func (e *ddlApplyEnv) lockTable(t *testing.T, table string) func() {
	ctx := context.Background()
	tx, err := e.dstConn.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE", table))
	require.NoError(t, err)

	var once sync.Once
	unlock := func() {
		once.Do(func() {
			_ = tx.Commit(ctx)
		})
	}
	t.Cleanup(unlock)
	return unlock
}

func (e *ddlApplyEnv) hasPrimaryKey(t *testing.T, table string) bool {
	var exists bool
	require.NoError(t, e.dstConn.QueryRow(
		context.Background(),
		"SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE contype = 'p' AND conrelid = $1::regclass)",
		table,
	).Scan(&exists))
	return exists
}

func requireCodedError(t *testing.T, err error, code coded.Code) {
	require.Error(t, err)
	var codedErr coded.CodedError
	require.ErrorAs(t, err, &codedErr)
	require.Equal(t, code, codedErr.Code())
}

func TestApplyCommands_PermissionDenied(t *testing.T) {
	env := newDDLApplyEnv(t, "ddl_perm.t")
	env.execSrc(t,
		`CREATE SCHEMA ddl_perm`,
		`CREATE TABLE ddl_perm.t (id INT PRIMARY KEY)`,
	)
	env.execDst(t,
		`CREATE SCHEMA ddl_perm`,
		`DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ddl_limited') THEN
				CREATE ROLE ddl_limited LOGIN PASSWORD 'ddl_limited';
			END IF;
		END $$`,
	)
	// The transfer user has no CREATE privilege on schema ddl_perm of the target: SQLSTATE 42501.
	env.dst.User = "ddl_limited"
	env.dst.Password = model.SecretString("ddl_limited")

	err := env.apply(t, provider_postgres.PgObjectTypeTable)
	requireCodedError(t, err, error_codes.PostgresDDLPermissionDenied)
}

func TestApplyCommands_UndefinedObject(t *testing.T) {
	env := newDDLApplyEnv(t, "ddl_undef.t")
	env.execSrc(t,
		`CREATE SCHEMA ddl_undef`,
		`CREATE TYPE ddl_undef.mood AS ENUM ('ok', 'bad')`,
		`CREATE TABLE ddl_undef.t (id INT PRIMARY KEY, mood ddl_undef.mood)`,
	)
	// The target has the schema but not the user-defined type the table depends on: SQLSTATE 42704.
	env.execDst(t, `CREATE SCHEMA ddl_undef`)

	err := env.apply(t, provider_postgres.PgObjectTypeTable)
	requireCodedError(t, err, error_codes.PostgresDDLUndefinedObject)
}

func TestApplyCommands_LockTimeoutRetried(t *testing.T) {
	env := newDDLApplyEnv(t, "ddl_lock_retry.t")
	env.execSrc(t,
		`CREATE SCHEMA ddl_lock_retry`,
		`CREATE TABLE ddl_lock_retry.t (id INT PRIMARY KEY)`,
	)
	env.execDst(t,
		`CREATE SCHEMA ddl_lock_retry`,
		`CREATE TABLE ddl_lock_retry.t (id INT)`,
	)
	env.setLockTimeout(t, "500ms")

	// The first attempt to add the primary key fails with SQLSTATE 55P03; the lock is released
	// before the retry, so ApplyCommands must succeed without an error.
	unlock := env.lockTable(t, "ddl_lock_retry.t")
	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(3 * time.Second)
		unlock()
	}()

	err := env.apply(t, provider_postgres.PgObjectTypePrimaryKey)
	<-released
	require.NoError(t, err)
	require.True(t, env.hasPrimaryKey(t, "ddl_lock_retry.t"))
}

func TestApplyCommands_LockTimeout(t *testing.T) {
	env := newDDLApplyEnv(t, "ddl_lock_fail.t")
	env.execSrc(t,
		`CREATE SCHEMA ddl_lock_fail`,
		`CREATE TABLE ddl_lock_fail.t (id INT PRIMARY KEY)`,
	)
	env.execDst(t,
		`CREATE SCHEMA ddl_lock_fail`,
		`CREATE TABLE ddl_lock_fail.t (id INT)`,
	)
	env.setLockTimeout(t, "500ms")

	// The lock is held during all retries: SQLSTATE 55P03 must be reported with a dedicated code.
	unlock := env.lockTable(t, "ddl_lock_fail.t")
	err := env.apply(t, provider_postgres.PgObjectTypePrimaryKey)
	unlock()
	requireCodedError(t, err, error_codes.PostgresDDLLockTimeout)
	require.False(t, env.hasPrimaryKey(t, "ddl_lock_fail.t"))
}
