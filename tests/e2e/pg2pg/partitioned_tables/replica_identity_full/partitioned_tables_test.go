package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/yatestx"
)

var tableNames = []string{"parttable", "parttable_y2026m01", "parttable_y2026m02"}

func newTransfer(t *testing.T) *model.Transfer {
	t.Helper()
	t.Setenv("YC", "1")
	source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir(yatestx.ProjectSource("dump")))
	target := pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
	target.Cleanup = model.Drop
	require.False(t, source.UseFakePrimaryKey)
	return transferhelpers.MakeTransfer(transferhelpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
}

func sourceStorage(t *testing.T, transfer *model.Transfer) *provider_postgres.Storage {
	t.Helper()
	source := transfer.Src.(*provider_postgres.PgSource)
	storage, err := provider_postgres.NewStorage(source.ToStorageParams(nil))
	require.NoError(t, err)
	t.Cleanup(storage.Close)
	return storage
}

func TestReplicaIdentityFullSchema(t *testing.T) {
	storage := sourceStorage(t, newTransfer(t))
	t.Run("all tables", func(t *testing.T) {
		schema, err := storage.LoadSchema()
		require.NoError(t, err)
		for _, name := range tableNames {
			checkFullIdentity(t, schema[abstract.TableID{Namespace: "public", Name: name}])
		}
	})
	for _, name := range tableNames {
		t.Run(name, func(t *testing.T) {
			schema, err := storage.TableSchema(context.Background(), abstract.TableID{Namespace: "public", Name: name})
			require.NoError(t, err)
			checkFullIdentity(t, schema)
		})
	}
}

func checkFullIdentity(t *testing.T, schema *abstract.TableSchema) {
	t.Helper()
	require.NotNil(t, schema)
	require.Len(t, schema.Columns(), 3)
	for _, column := range schema.Columns() {
		require.True(t, column.PrimaryKey, "column %s must participate in the full identity", column.ColumnName)
		require.True(t, column.FakeKey, "column %s is not a real primary key", column.ColumnName)
	}
}

func TestReplicaIdentityFullSnapshotAndReplication(t *testing.T) {
	transfer := newTransfer(t)
	source := sourceStorage(t, transfer)
	target, err := provider_postgres.NewStorage(transfer.Dst.(*provider_postgres.PgDestination).ToStorageParams())
	require.NoError(t, err)
	defer target.Close()

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	checkRows := func() {
		t.Helper()
		for _, name := range tableNames {
			query := fmt.Sprintf(`SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id), '[]'::jsonb)::text FROM public.%s t`, name)
			var expected string
			require.NoError(t, source.Conn.QueryRow(context.Background(), query).Scan(&expected))
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				var actual string
				err := target.Conn.QueryRow(context.Background(), query).Scan(&actual)
				require.NoError(c, err)
				require.Equal(c, expected, actual, "table %s", name)
			}, 60*time.Second, 100*time.Millisecond)
		}
	}

	checkRows() // Snapshot must preserve the partition tree and every value, including NULL.
	for _, query := range []string{
		`INSERT INTO parttable VALUES (3, '2026-01-20', NULL), (4, '2026-02-20', 'inserted')`,
		`UPDATE parttable SET value = 'updated from null' WHERE id = 2`,
		`DELETE FROM parttable WHERE id = 3`,
		`UPDATE parttable SET logdate = '2026-02-15' WHERE id = 1`,
	} {
		_, err := source.Conn.Exec(context.Background(), query)
		require.NoError(t, err)
		checkRows()
	}
}

func TestPartitionWithoutReplicaIdentityFull(t *testing.T) {
	transfer := newTransfer(t)
	source := sourceStorage(t, transfer)
	_, err := source.Conn.Exec(context.Background(), `ALTER TABLE parttable_y2026m02 REPLICA IDENTITY DEFAULT`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := source.Conn.Exec(context.Background(), `ALTER TABLE parttable_y2026m02 REPLICA IDENTITY FULL`)
		require.NoError(t, err)
	})

	worker, err := delivery.ActivateErr(transfer)
	if worker != nil {
		defer worker.Close(t)
	}
	require.ErrorContains(t, err, `PRIMARY KEY check failed: ["public"."parttable_y2026m02"]: no key columns found`)
}
