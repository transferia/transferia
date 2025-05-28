package alters

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestFlag(t *testing.T) {
	sinks := model.KnownDestinations()
	for _, sinkType := range sinks {
		p, ok := model.DestinationF(abstract.ProviderType(sinkType))
		require.Truef(t, ok, "Unknown destination type %s", sinkType)
		prov := p()
		if _, ok := prov.(model.AlterableDestination); !ok {
			continue
		}
		t.Run(sinkType, func(t *testing.T) {
			checkSchemaFlag(t, prov)
		})
	}
}

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

func checkSchemaFlag(t *testing.T, i model.Destination) {
	val := reflect.ValueOf(i)

	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	for val.Kind() == reflect.Interface && !val.IsNil() {
		val = val.Elem()
	}
	// hack for yt destination wrapper
	if strings.Contains(val.String(), "Wrapper") {
		val = val.FieldByName("Model").Elem()
	}

	field := val.FieldByName("IsSchemaMigrationDisabled")
	if !field.IsValid() {
		t.Errorf("Field IsSchemaMigrationDisabled not found in %s", val.String())
	}
	require.Equal(t, reflect.Bool, field.Kind())
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
