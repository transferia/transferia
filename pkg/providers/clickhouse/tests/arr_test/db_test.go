package arr_test_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/middlewares"
	"github.com/transferria/transferria/pkg/providers/clickhouse"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	Source = model.MockSource{}
	Target = *chrecipe.MustTarget(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("init.sql"))
)

func TestCHArray(t *testing.T) {
	Source.WithDefaults()
	Target.WithDefaults()
	schema := abstract.NewTableSchema([]abstract.ColSchema{
		{
			TableName:    "test",
			ColumnName:   "arr",
			DataType:     "any",
			OriginalType: "ch:Array(UInt32)",
		},
		{
			TableName:    "test",
			ColumnName:   "arr_n",
			DataType:     "any",
			OriginalType: "ch:Array(Nullable(UInt32))",
		},
		{
			TableName:    "test",
			ColumnName:   "arr_arr_int",
			DataType:     "any",
			OriginalType: "ch:Array(Array(Int32))",
		},
		{
			TableName:    "test",
			ColumnName:   "arr_arr_str",
			DataType:     "any",
			OriginalType: "ch:Array(Array(String))",
		},
	})
	v1 := uint32(1)
	v2 := uint32(2)

	items := []abstract.ChangeItem{
		{
			Kind:        abstract.InsertKind,
			Table:       "test",
			ColumnNames: []string{"arr", "arr_n", "arr_arr_int", "arr_arr_str"},
			ColumnValues: []any{[]uint32{1, 2}, []any{uint32(3), uint32(4)},
				[][]any{{int32(1), int32(2)}, {int32(3)}},
				[][]any{{[]byte("foo"), "bar"}}},
			TableSchema: schema,
		},
		{
			Kind:         abstract.InsertKind,
			Table:        "test",
			ColumnNames:  []string{"arr", "arr_n", "arr_arr_int", "arr_arr_str"},
			ColumnValues: []any{[]any{&v1, &v2}, []any{uint32(1), nil}, []any{}, [][]any(nil)},
			TableSchema:  schema,
		},
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)
	sinker, err := clickhouse.NewSink(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), new(abstract.LocalRuntime), middlewares.MakeConfig())
	require.NoError(t, err)
	require.NoError(t, sinker.Push(items))
}
