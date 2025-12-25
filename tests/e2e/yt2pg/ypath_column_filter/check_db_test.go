package snapshot

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	IncludedCol = "included_col"
	ExcludedCol = "excluded_col"
	TableName   = "test_table"
	TablePath   = "//home/cdc/junk"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = yt_provider.YtSource{
		Cluster: os.Getenv("YT_PROXY"),
		YtProxy: os.Getenv("YT_PROXY"),
		Paths:   []string{TablePath},
		YtToken: "",
	}
	dstPort, _ = strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	Target     = postgres.PgDestination{
		Hosts:     []string{"localhost"},
		ClusterID: os.Getenv("TARGET_CLUSTER_ID"),
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      dstPort,
		Cleanup:   model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

var TestData = map[string]interface{}{IncludedCol: 1, ExcludedCol: 0}

var YtColumns = []schema.Column{
	{Name: IncludedCol, ComplexType: schema.TypeInt8, SortOrder: schema.SortAscending},
	{Name: ExcludedCol, ComplexType: schema.TypeInt8},
}

func TestSnapshot(t *testing.T) {
	fillSource(t)

	doColumnFilterSnapshot(t, 1, "Test correct ypath", fmt.Sprintf("%s/%s{%s}", TablePath, TableName, IncludedCol))
	doColumnFilterSnapshot(t, 2, "Test incorrect ypath", fmt.Sprintf("%s{%s}", TablePath, IncludedCol))
}

func fillSource(t *testing.T) {
	ytc, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, &yt.Config{Proxy: Source.YtProxy})
	require.NoError(t, err)

	sch := schema.Schema{
		Strict:     nil,
		UniqueKeys: false,
		Columns:    YtColumns,
	}

	ctx := context.Background()
	tablePath := ypath.Path(TablePath).Child(TableName)
	wr, err := yt.WriteTable(ctx, ytc, tablePath, yt.WithCreateOptions(yt.WithSchema(sch), yt.WithRecursive()))
	require.NoError(t, err)

	require.NoError(t, wr.Write(TestData))
	require.NoError(t, wr.Commit())
}

func doColumnFilterSnapshot(t *testing.T, columnC int, testName, dataObject string) {
	t.Run(testName, func(t *testing.T) {
		transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
		transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{dataObject}}

		snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
		require.NoError(t, snapshotLoader.UploadV2(context.Background(), nil, nil))

		pgTarget := helpers.GetSampleableStorageByModel(t, Target)
		totalInserts := 0
		require.NoError(t, pgTarget.LoadTable(context.Background(), abstract.TableDescription{
			Name:   TableName,
			Schema: "public",
		}, func(input []abstract.ChangeItem) error {
			for _, ci := range input {
				if ci.Kind != abstract.InsertKind {
					continue
				}
				require.True(t, len(ci.ColumnNames) == columnC)
				require.True(t, len(ci.ColumnValues) == columnC)
				totalInserts += 1
			}
			return nil
		}))
		require.Equal(t, 1, totalInserts)
	})
}
