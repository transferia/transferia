package cleanup

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_sample "github.com/transferia/transferia/pkg/providers/sample"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	Path      = "//home/cdc/test/sample2yt_e2e"
	TablePath = ypath.Path(Path).Child("__test1")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestSnapshot(t *testing.T) {
	ctx := context.Background()
	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	src := provider_sample.RecipeSource()
	src.SnapshotEventCount = 800000
	src.MaxSampleData = 50000

	dst := helpers_yt.RecipeYtTarget(Path).(*provider_yt.YtDestinationWrapper)
	dst.Model.Cleanup = model.Replace

	createTmpTables(t, ctx, ytEnv.YT)
	require.Equal(t, 1, nodeCount(t, ctx, ytEnv.YT))

	transfer := helpers.MakeTransfer(helpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
	require.NoError(t, tasks.CleanupResource(ctx, model.TransferOperation{}, *transfer, logger.Log, coordinator.NewFakeClient()))

	require.Equal(t, 0, nodeCount(t, ctx, ytEnv.YT))
}

func createTmpTables(t *testing.T, ctx context.Context, client yt.Client) {
	tmpPath := model.MakeTmpTableName(TablePath.String(), helpers.TransferID, model.TmpTableSuffix)

	trueVal := true
	schema := schema.Schema{
		UniqueKeys: true,
		Strict:     &trueVal,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}

	tables := map[ypath.Path]migrate.Table{
		ypath.Path(tmpPath): {
			Schema:     schema,
			Attributes: map[string]any{},
		},
	}

	require.NoError(t, migrate.EnsureTables(ctx, client, tables, migrate.OnConflictFail))
}

func nodeCount(t *testing.T, ctx context.Context, client yt.Client) int {
	var tables []struct {
		Name string `yson:",value"`
	}

	require.NoError(t, client.ListNode(ctx, ypath.Path(Path), &tables, &yt.ListNodeOptions{}))
	return len(tables)
}
