package rawgroupertransformerwithstat

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
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
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
	Target = provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:                     "//home/cdc/test/pg2yt_e2e",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		UseStaticTableOnSnapshot: true,
	})
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
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)
	require.NoError(t, transfer.TransformationFromJSON(`
{
	"transformers": [
	  {
		"rawDocGrouper": {
		  "tables": {
			"includeTables": [
				"^public.__test$"
			]
		  },
		  "keys": [
				"aid",
				"id",
                "ts",
                "etl_updated_at"
			],
          "fields": [
                  "str"
           ]
		}
	  }
	]
}
`))
	worker := delivery.Activate(t, transfer)
	require.NotNil(t, worker, "Transfer is not activated")

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "__test",
		storagecomparison.GetSampleableStorageByModel(t, Source),
		storagecomparison.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	_, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)

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
			require.Len(t, row.TableSchema.Columns(), 6)
			require.Equal(t, []string{"aid", "id", "ts", "etl_updated_at", "str", "doc"}, row.ColumnNames)
		}
		return nil
	}))
}
