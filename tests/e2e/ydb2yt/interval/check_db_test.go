package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_storage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb/recipe"
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
)

const ydbTableName = "test_table"

func execDDL(t *testing.T, ydbConn *ydb_go_sdk.Driver, query string) {
	foo := func(ctx context.Context, session ydb_table.Session) (err error) {
		return session.ExecuteSchemeQuery(ctx, query)
	}
	require.NoError(t, ydbConn.Table().Do(context.Background(), foo))
}

func execQuery(t *testing.T, ydbConn *ydb_go_sdk.Driver, query string) {
	foo := func(ctx context.Context, session ydb_table.Session) error {
		writeTx := ydb_table.TxControl(ydb_table.BeginTx(ydb_table.WithSerializableReadWrite()), ydb_table.CommitTx())
		_, _, err := session.Execute(ctx, writeTx, query, nil)
		return err
	}
	require.NoError(t, ydbConn.Table().Do(context.Background(), foo))
}

func TestGroup(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	dst := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:          "//home/cdc/test/pg2yt_e2e",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	sourcePort, err := helpers.GetPortFromStr(src.Instance)
	require.NoError(t, err)
	targetPort, err := helpers.GetPortFromStr(dst.Cluster())
	require.NoError(t, err)
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "YDB source", Port: sourcePort},
		helpers.LabeledPort{Label: "YT target", Port: targetPort},
	))

	t.Run("fill source", func(t *testing.T) {
		ydbConn := ydbrecipe.Driver(t)
		transferhelpers.InitSrcDst(transferhelpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)

		execDDL(t, ydbConn, fmt.Sprintf(`
			--!syntax_v1
			CREATE TABLE %s (
				id     Int64 NOT NULL,
				value  Interval,
				PRIMARY KEY (id)
			);
		`, ydbTableName))

		execQuery(t, ydbConn, fmt.Sprintf(`
			--!syntax_v1
			INSERT INTO %s (id, value) VALUES
				(1, DateTime::IntervalFromMicroseconds(1)),
				(2, null),
				(3, DateTime::IntervalFromMicroseconds(123000)),
				(4, DateTime::IntervalFromMicroseconds(4291660800000000)),
				(5, DateTime::IntervalFromMicroseconds(31536000000000)),
				(6, DateTime::IntervalFromMicroseconds(7862400000000));
		`, ydbTableName))

		require.NoError(t, helpers.WaitDestinationEqualRowsCount("", ydbTableName, helpers.GetSampleableStorageByModel(t, src), 600*time.Second, 6))
	})

	t.Run("snapshot", func(t *testing.T) {
		transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
		helpers.Activate(t, transfer)
		require.NoError(t, helpers.WaitDestinationEqualRowsCount("", ydbTableName, helpers.GetSampleableStorageByModel(t, dst), 600*time.Second, 6))
	})

	t.Run("canon", func(t *testing.T) {
		ytStorageParams := provider_yt.YtStorageParams{
			Token:   dst.Token(),
			Cluster: os.Getenv("YT_PROXY"),
			Path:    dst.Path(),
		}
		st, err := yt_storage.NewStorage(&ytStorageParams)
		require.NoError(t, err)

		var data []helpers.CanonTypedChangeItem
		require.NoError(t, st.LoadTable(context.Background(), abstract.TableDescription{Schema: "", Name: ydbTableName},
			func(input []abstract.ChangeItem) error {
				for _, row := range input {
					if row.Kind == abstract.InsertKind {
						data = append(data, helpers.ToCanonTypedChangeItem(row))
					}
				}
				return nil
			},
		))
		canon.SaveJSON(t, data)
	})
}
