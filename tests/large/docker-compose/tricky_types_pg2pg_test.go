package dockercompose

import (
	"context"
	_ "embed"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	trickyTypesPg2PgSource = postgres.PgSource{
		Hosts:    []string{"localhost"},
		User:     "postgres",
		Password: "123",
		Database: "postgres",

		PgDumpCommand: dockerPgDump,
	}
	trickyTypesPg2PgTarget = postgres.PgDestination{
		Hosts:    []string{"localhost"},
		User:     "postgres",
		Password: "123",
		Database: "postgres",

		Cleanup:            model.Drop,
		DisableSQLFallback: true,
	}

	//go:embed data/tricky_types_pg2pg/source1_increment.sql
	source1IncrementSQL string
	//go:embed data/tricky_types_pg2pg/source4_increment.sql
	source4IncrementSQL string
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, &trickyTypesPg2PgSource, &trickyTypesPg2PgTarget, abstract.TransferTypeSnapshotAndIncrement)
}

type CanonData struct {
	AfterSnapshot  string `json:"after_snapshot"`
	AfterIncrement string `json:"after_increment"`
}

func TestTrickyTypesPg2PgSupportedTypes(t *testing.T) {
	t.Parallel()

	dumpTargetDB := func() string {
		return pgrecipe.PgDump(
			t,
			[]string{"docker", "exec", "docker-compose_tricky-types-pg2pg-target1_1", "pg_dump", "--table", "public.pgis_supported_types"},
			[]string{"docker", "exec", "docker-compose_tricky-types-pg2pg-target1_1", "psql"},
			"user=postgres dbname=postgres password=123 host=localhost port=6432",
			"public.pgis_supported_types",
		)
	}

	sourceCopy := trickyTypesPg2PgSource
	sourceCopy.DBTables = []string{"public.pgis_supported_types"}
	sourceCopy.Port = 5432
	targetCopy := trickyTypesPg2PgTarget
	targetCopy.CopyUpload = true
	targetCopy.Port = 6432
	transfer := helpers.MakeTransfer(helpers.TransferID, &sourceCopy, &targetCopy, abstract.TransferTypeSnapshotAndIncrement)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	var canonData CanonData
	canonData.AfterSnapshot = dumpTargetDB()

	conn, err := pgx.Connect(context.Background(), "user=postgres dbname=postgres password=123 host=localhost port=5432")
	require.NoError(t, err)
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), source1IncrementSQL)
	require.NoError(t, err)

	err = helpers.WaitEqualRowsCount(t, "public", "pgis_supported_types", helpers.GetSampleableStorageByModel(t, sourceCopy), helpers.GetSampleableStorageByModel(t, targetCopy), 30*time.Second)
	require.NoError(t, err)
	canonData.AfterIncrement = dumpTargetDB()
	canon.SaveJSON(t, &canonData)
}

func TestTrickyTypesPg2PgSupportedTypesDontWorkUnlessBinarySerializationIsUsed(t *testing.T) {
	t.Parallel()

	sourceCopy := trickyTypesPg2PgSource
	sourceCopy.Port = 5433
	sourceCopy.SnapshotSerializationFormat = postgres.PgSerializationFormatText
	sourceCopy.DBTables = []string{"public.pgis_supported_types"}
	targetCopy := trickyTypesPg2PgTarget
	targetCopy.Port = 6433
	targetCopy.DisableSQLFallback = true
	transfer := helpers.MakeTransfer(helpers.TransferID, &sourceCopy, &targetCopy, abstract.TransferTypeSnapshotOnly)

	_, err := helpers.ActivateErr(transfer)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid endian flag value encountered")
}

func TestTrickyTypesPg2PgUnsupportedTypes(t *testing.T) {
	t.Parallel()

	sourceCopy := trickyTypesPg2PgSource
	sourceCopy.Port = 5434
	sourceCopy.SnapshotSerializationFormat = postgres.PgSerializationFormatText
	sourceCopy.DBTables = []string{"public.pgis_box3d_unsupported", "public.pgis_box2d_unsupported"}
	targetCopy := trickyTypesPg2PgTarget
	targetCopy.Port = 6434
	targetCopy.DisableSQLFallback = true
	transfer := helpers.MakeTransfer(helpers.TransferID, &sourceCopy, &targetCopy, abstract.TransferTypeSnapshotOnly)

	_, err := helpers.ActivateErr(transfer)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no binary input function available for type")
}

func TestTrickyTypesPg2PgTemporals(t *testing.T) {
	t.Parallel()

	dumpTargetDB := func() string {
		return pgrecipe.PgDump(
			t,
			[]string{"docker", "exec", "docker-compose_tricky-types-pg2pg-target1_1", "pg_dump", "--table", "public.temporals"},
			[]string{"docker", "exec", "docker-compose_tricky-types-pg2pg-target1_1", "psql"},
			"user=postgres dbname=postgres password=123 host=localhost port=6432",
			"public.temporals",
		)
	}

	sourceCopy := trickyTypesPg2PgSource
	sourceCopy.Port = 5435
	sourceCopy.DBTables = []string{"public.temporals"}
	targetCopy := trickyTypesPg2PgTarget
	targetCopy.CopyUpload = true
	targetCopy.Port = 6432
	transfer := helpers.MakeTransfer(helpers.TransferID, &sourceCopy, &targetCopy, abstract.TransferTypeSnapshotAndIncrement)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	var canonData CanonData
	canonData.AfterSnapshot = dumpTargetDB()

	conn, err := pgx.Connect(context.Background(), "user=postgres dbname=postgres password=123 host=localhost port=5435")
	require.NoError(t, err)
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), source4IncrementSQL)
	require.NoError(t, err)

	err = helpers.WaitEqualRowsCount(t, "public", "temporals", helpers.GetSampleableStorageByModel(t, sourceCopy), helpers.GetSampleableStorageByModel(t, targetCopy), 30*time.Second)
	require.NoError(t, err)
	canonData.AfterIncrement = dumpTargetDB()
	canon.SaveJSON(t, &canonData)
}
