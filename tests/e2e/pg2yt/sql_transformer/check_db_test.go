package sqltransformer

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
	_ "github.com/transferia/transferia/pkg/transformer/registry/clickhouse"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
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
	Source.WithDefaults()
}

func TestGroup(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga

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
	t.Setenv("CH_LOCAL_PATH", os.Getenv("RECIPE_CLICKHOUSE_BIN"))

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)
	require.NoError(t, transfer.TransformationFromJSON(`
  {
    "transformers":  [
      {
        "sql":  {
          "tables":  {
            "includeTables":  [
              "^public.__test$"
            ]
          },
          "query":  "SELECT\r\n    id,\r\n    parseDateTime32BestEffort( JSONExtractString(data, 'eventTime')) AS eventTime,\r\n    JSONExtractString(data, 'sourceIPAddress') AS sourceIPAddress,\r\n    JSONExtractString(data, 'userAgent') AS userAgent,\r\n    JSONExtractString(data, 'requestParameters.bucketName') AS bucketName,\r\n    JSONExtractString(data, 'additionalEventData.SignatureVersion') AS signatureVersion,\r\n    JSONExtractString(data, 'additionalEventData.CipherSuite') AS cipherSuite,\r\n    JSONExtractUInt(data, 'additionalEventData.bytesTransferredIn') AS bytesTransferredIn,\r\n    JSONExtractString(data, 'additionalEventData.AuthenticationMethod') AS authenticationMethod,\r\n    JSONExtractUInt(data, 'additionalEventData.bytesTransferredOut') AS bytesTransferredOut,\r\n    JSONExtractString(data, 'requestID') AS requestID,\r\n    JSONExtractString(data, 'eventID') AS eventID,\r\n    JSONExtractBool(data, 'readOnly') AS readOnly,\r\n    JSONExtractString(data, 'resources[1].ARN') AS resourceARN,\r\n    JSONExtractString(data, 'eventType') AS eventType,\r\n    JSONExtractBool(data, 'managementEvent') AS managementEvent\r\nFROM table;\r\n"
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
			require.Len(t, row.TableSchema.Columns(), 16)
			require.Equal(
				t,
				[]string{"id", "eventTime", "sourceIPAddress", "userAgent", "bucketName", "signatureVersion", "cipherSuite", "bytesTransferredIn", "authenticationMethod", "bytesTransferredOut", "requestID", "eventID", "readOnly", "resourceARN", "eventType", "managementEvent"},
				row.ColumnNames,
			)
		}
		return nil
	}))
}
