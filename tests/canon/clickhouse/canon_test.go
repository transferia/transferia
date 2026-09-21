package clickhouse

import (
	"sort"
	"strings"
	"testing"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_clickhouse "github.com/transferia/transferia/pkg/providers/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/tests/canon/validator"
	"github.com/transferia/transferia/tests/helpers/delivery"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
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
	Source := &clickhouse_model.ChSource{
		ShardsList: []clickhouse_model.ClickHouseShard{
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
		HTTPPort:   testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort: testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	}
	Source.WithDefaults()

	transfer := transferhelpers.MakeTransfer(
		transferhelpers.TransferID,
		Source,
		&model.MockDestination{
			SinkerFactory: validator.New(
				model.IsStrictSource(Source),
				validator.InitDone(t),
				validator.ValuesTypeChecker,
				validator.Canonizator(t, sortItems),
				validator.TypesystemChecker(provider_clickhouse.ProviderType, getBaseType),
			),
			Cleanup: model.DisabledCleanup,
		},
		abstract.TransferTypeSnapshotOnly,
	)
	_ = delivery.Activate(t, transfer)
}
