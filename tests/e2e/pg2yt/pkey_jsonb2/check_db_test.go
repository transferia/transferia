package pkeyjsonb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	pg_provider "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = pg_provider.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.permalinks_setup", "public.permalinks_setup2", "public.done"},
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e_pkey_jsonb")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Target.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------

func jsonSerDeUdf(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
	newChangeItems := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)
	for i := range items {
		if items[i].Kind == abstract.UpdateKind {
			currJSON := items[i].ToJSONString()
			outChangeItem, err := abstract.UnmarshalChangeItem([]byte(currJSON))
			if err != nil {
				errors = append(errors, abstract.TransformerError{
					Input: items[i],
					Error: err,
				})
			}
			newChangeItems = append(newChangeItems, *outChangeItem)
		} else {
			newChangeItems = append(newChangeItems, items[i])
		}
	}
	return abstract.TransformerResult{
		Transformed: newChangeItems,
		Errors:      errors,
	}
}

func suitableTablesUdf(table abstract.TableID, schema abstract.TableColumns) bool {
	return table.Name == "permalinks_setup"
}

//---------------------------------------------------------------------------------------------------------------------

func TestSnapshotAndIncrement(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, TransferType)
	jsonSerDeTransformer := helpers.NewSimpleTransformer(t, jsonSerDeUdf, suitableTablesUdf)
	helpers.AddTransformer(t, transfer, jsonSerDeTransformer)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//------------------------------------------------------------------------------

	srcConn, err := pg_provider.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), "UPDATE public.permalinks_setup SET version_id = 2515991415;")
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), "UPDATE public.permalinks_setup2 SET version_id = 2515991415;")
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), "INSERT INTO done VALUES (0);")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "done", helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, 1))
	helpers.CheckRowsCount(t, Target.LegacyModel(), "public", "permalinks_setup", 1)
	helpers.CheckRowsCount(t, Target.LegacyModel(), "public", "permalinks_setup2", 1)
}
