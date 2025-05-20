package alters

import (
	"os"
	"testing"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	_ "github.com/transferia/transferia/pkg/dataplane"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/sink"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestAllSinks(t *testing.T) {
	sinks := model.KnownDestinations()

	changeItems := []abstract.ChangeItem{
		{
			ID:           1,
			Kind:         abstract.InsertKind,
			Table:        "test",
			ColumnNames:  []string{"id", "name"},
			ColumnValues: []interface{}{1, "John Doe"},
			TableSchema: changeitem.NewTableSchema([]changeitem.ColSchema{
				changeitem.NewColSchema("id", schema.TypeInt64, true),
				changeitem.NewColSchema("name", schema.TypeString, false),
			}),
		},
		{
			ID:           1,
			Kind:         abstract.InsertKind,
			Table:        "test",
			ColumnNames:  []string{"id", "name", "lastName"},
			ColumnValues: []interface{}{2, "John", "Doe"},
			TableSchema: changeitem.NewTableSchema([]changeitem.ColSchema{
				changeitem.NewColSchema("id", schema.TypeInt64, true),
				changeitem.NewColSchema("name", schema.TypeString, false),
				changeitem.NewColSchema("lastName", schema.TypeString, false),
			}),
		},
	}

	for _, sinkType := range sinks {
		target, err := getAlterableDestination(t, abstract.ProviderType(sinkType))
		if err != nil {
			t.Fatalf("Failed to create recipe destination: %v", err)
		}
		if target == nil {
			continue
		}
		transfer := &model.Transfer{
			Dst: target,
		}
		time.Sleep(10 * time.Second)
		t.Run(sinkType, func(t *testing.T) {
			r := solomon.NewRegistry(solomon.NewRegistryOpts())
			sink, err := sink.ConstructBaseSink(
				transfer,
				logger.Log,
				r, // metrics registry
				coordinator.NewFakeClient(),
				middlewares.Config{},
			)
			if err != nil {
				t.Errorf("Failed to create sink %s: %v", sinkType, err)
				return
			}
			for _, ci := range changeItems {
				err = sink.Push([]abstract.ChangeItem{ci})
				if err != nil {
					t.Errorf("Failed to push to sink %s: %v", sinkType, err)
				}
			}

			err = sink.Close()
			if err != nil {
				t.Errorf("Failed to close sink %s: %v", sinkType, err)
			}
		})
	}
}

func getAlterableDestination(t *testing.T, sinkType abstract.ProviderType) (model.Destination, error) {
	p, ok := model.DestinationF(sinkType)
	if !ok {
		return nil, xerrors.Errorf("Unknown sink type: %s", sinkType)
	}
	prov := p()
	if _, ok := prov.(model.AlterableDestination); !ok {
		return nil, nil
	}
	switch sinkType {
	case postgres.ProviderType:
		return pgrecipe.RecipeTarget(), nil
	case mysql.ProviderType:
		return helpers.RecipeMysqlTarget(), nil
	case clickhouse.ProviderType:
		return chrecipe.MustTarget(chrecipe.WithInitFile("data/ch.sql"), chrecipe.WithDatabase("test"), chrecipe.WithPrefix("DB0_")), nil
	case ydb.ProviderType:
		dst := ydb.YdbDestination{
			Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
			Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
			Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		}
		dst.WithDefaults()
		return &dst, nil
	case yt.ProviderType:
		target := yt_helpers.RecipeYtTarget("//home/cdc/test/alters")
		target.AllowAlter()
		return target, nil
	default:
		return nil, xerrors.Errorf("Unknown sink type: %s", sinkType)
	}
}
