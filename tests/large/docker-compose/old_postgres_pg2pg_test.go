package dockercompose

import (
	"testing"

	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	oldPostgresPg2PgSource = postgres.PgSource{
		Hosts:    []string{"localhost"},
		User:     "postgres",
		Password: "123",
		Database: "postgres",
		DBTables: []string{"public.test_table"},
		Port:     8432,

		PgDumpCommand: dockerPgDump,
	}
	oldPostgresPg2PgTarget = postgres.PgDestination{
		Hosts:      []string{"localhost"},
		User:       "postgres",
		Password:   "123",
		Database:   "postgres",
		CopyUpload: true,
		Port:       8433,

		DisableSQLFallback: true,
	}
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, &oldPostgresPg2PgSource, &oldPostgresPg2PgTarget, abstract.TransferTypeSnapshotOnly)
}

func TestOldPostgresPg2Pg(t *testing.T) {
	t.Parallel()

	dumpTargetDB := func() string {
		return pgrecipe.PgDump(
			t,
			[]string{"docker", "run", "--network", "host", "registry.yandex.net/data-transfer/tests/base:1@sha256:48a92174b2d5917fbac6be0a48d974e3f836338acf4fa03f74fcfea7437386f1", "pg_dump", "--table", "public.test_table"},
			[]string{"docker", "run", "--network", "host", "registry.yandex.net/data-transfer/tests/base:1@sha256:48a92174b2d5917fbac6be0a48d974e3f836338acf4fa03f74fcfea7437386f1", "psql"},
			"user=postgres dbname=postgres password=123 host=localhost port=8433",
			"public.test_table",
		)
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, &oldPostgresPg2PgSource, &oldPostgresPg2PgTarget, abstract.TransferTypeSnapshotAndIncrement)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	var canonData CanonData
	canonData.AfterSnapshot = dumpTargetDB()
	canon.SaveJSON(t, &canonData)
}
