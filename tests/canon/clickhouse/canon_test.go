package clickhouse

import (
	"sort"
	"strings"
	"testing"

	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/tests/canon/validator"
	"github.com/transferia/transferia/tests/helpers"
)

func getID(item abstract.ChangeItem) uint64 {
	var index int
	var found bool
	for i, name := range item.ColumnNames {
		if name == "id" {
			index = i
			found = true
			break
		}
	}

	if !found {
		return 0
	}

	id, ok := item.ColumnValues[index].(uint64)
	if !ok {
		return 0
	}

	return id
}

func sortItems(item []abstract.ChangeItem) []abstract.ChangeItem {
	sort.Slice(item, func(i, j int) bool {
		return getID(item[i]) < getID(item[j])
	})
	return item
}

func getBaseType(colSchema abstract.ColSchema) string {
	return columntypes.BaseType(strings.TrimPrefix(colSchema.OriginalType, "ch:"))
}

func TestCanonSource(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga
	Source := &model.ChSource{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:       "default",
		Password:   "",
		Database:   "canon",
		HTTPPort:   helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort: helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	}
	Source.WithDefaults()

	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		Source,
		&dp_model.MockDestination{
			SinkerFactory: validator.New(
				dp_model.IsStrictSource(Source),
				validator.InitDone(t),
				validator.ValuesTypeChecker,
				validator.Canonizator(t, sortItems),
				validator.TypesystemChecker(clickhouse.ProviderType, getBaseType),
			),
			Cleanup: dp_model.DisabledCleanup,
		},
		abstract.TransferTypeSnapshotOnly,
	)
	_ = helpers.Activate(t, transfer)
}
