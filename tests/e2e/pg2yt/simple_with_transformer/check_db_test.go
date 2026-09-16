package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	srcPort = testenv.GetIntFromEnv("PG_LOCAL_PORT")
	Source  = provider_postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      srcPort,
		DBTables:  []string{"public.__test"},
	}
	Target = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestGroup(t *testing.T) {
	targetPort, err := network.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Load", Load)
	})
}

func Load(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)
	require.NoError(t, transfer.TransformationFromJSON(`
{
	"transformers": [
	  {
		"filterColumns": {
		  "tables": {
			"includeTables": [
				"^public.__test$"
			]
		  },
		  "columns": {
			"includeColumns": [
				"^f$",
				"^aid$",
				"^str$",
				"^id$",
				"^t$"
			]
		  }
		}
	  }
	]
}
`))
	worker := delivery.Activate(t, transfer)

	conn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "insert into __test (str, id, da, i) values ('qqq', 111, '1999-09-16', 1)")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "update __test set i=2 where str='qqq';")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), `insert into __test (str, id, da, i) values
                                                      ('www', 111, '1999-09-16', 1),
                                                      ('eee', 111, '1999-09-16', 1),
                                                      ('rrr', 111, '1999-09-16', 1)
    `)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "delete from __test where str='rrr';")
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	worker.Close(t)

	storage := storagecomparison.GetSampleableStorageByModel(t, Target.LegacyModel())
	require.NoError(t, storage.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "__test",
		Schema: "",
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}, func(items []abstract.ChangeItem) error {
		for _, row := range items {
			if !row.IsRowEvent() {
				continue
			}
			require.Len(t, row.TableSchema.Columns(), 5)
			require.Equal(t, []string{"aid", "str", "id", "f", "t"}, row.ColumnNames)
		}
		return nil
	}))
}
