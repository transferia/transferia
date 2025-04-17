package tests

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/dblog"
	"github.com/transferia/transferia/tests/helpers"
	mockstorage "github.com/transferia/transferia/tests/helpers/mock_storage"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	storage = CreateMockStorage()

	isSupporterKeyType = func(keyType string) dblog.TypeSupport {
		if keyType == "pg:text" || keyType == "pg:int" {
			return dblog.TypeSupported
		}

		return dblog.TypeUnsupported
	}

	converter = func(val interface{}, colSchema abstract.ColSchema) (string, error) {
		switch v := val.(type) {
		case string:
			return fmt.Sprintf("'%s'", v), nil
		case int:
			return fmt.Sprintf("'%d'", v), nil
		}

		return "", xerrors.Errorf("failed to convert value to string")
	}
)

func TestInferSizeChunkSize(t *testing.T) {
	t.Run("intersection case - null rows count", func(t *testing.T) {
		result, err := dblog.InferChunkSize(storage, abstract.TableID{Name: "nullRowsCount"}, dblog.DefaultChunkSizeInBytes)
		require.NoError(t, err)
		require.Equal(t, dblog.FallbackChunkSize, result)
	})
	t.Run("intersection case - null table size", func(t *testing.T) {
		result, err := dblog.InferChunkSize(storage, abstract.TableID{Name: "nullTableSize"}, dblog.DefaultChunkSizeInBytes)
		require.NoError(t, err)
		require.Equal(t, dblog.FallbackChunkSize, result)
	})
	t.Run("intersection case - not null rows count and table size", func(t *testing.T) {
		result, err := dblog.InferChunkSize(storage, abstract.TableID{Name: ""}, dblog.DefaultChunkSizeInBytes)
		require.NoError(t, err)
		require.Equal(t, dblog.DefaultChunkSizeInBytes, result)
	})
}

func TestMakeNextWhereStatement(t *testing.T) {
	primaryKey := []string{"int", "text"}
	lowBound := []string{"0", "qwe"}

	t.Run("intersection case - empty primary key", func(t *testing.T) {
		result := dblog.MakeNextWhereStatement(nil, lowBound)
		require.Equal(t, dblog.AlwaysTrueWhereStatement, result)
	})

	t.Run("intesection case - empty low bound", func(t *testing.T) {
		result := dblog.MakeNextWhereStatement(primaryKey, nil)
		require.Equal(t, dblog.AlwaysTrueWhereStatement, result)
	})

	t.Run("intersection case - filled low bound and primary key", func(t *testing.T) {
		result := dblog.MakeNextWhereStatement(primaryKey, lowBound)
		require.Equal(t, abstract.WhereStatement("(int,text) > (0,qwe)"), result)
	})
}

func TestMakeSQLTuple(t *testing.T) {
	t.Run("intersection case - empty array", func(t *testing.T) {
		result := dblog.MakeSQLTuple([]string{})
		require.Equal(t, "()", result)
	})
	t.Run("intersection case - 1 value in array", func(t *testing.T) {
		result := dblog.MakeSQLTuple([]string{"value"})
		require.Equal(t, "(value)", result)
	})
	t.Run("intersection case - more than 1 value in array", func(t *testing.T) {
		result := dblog.MakeSQLTuple([]string{"a", "b", "c"})
		require.Equal(t, "(a,b,c)", result)
	})
}

func TestPKeysToStringArr(t *testing.T) {
	arrColSchema, err := storage.TableSchema(context.TODO(), abstract.TableID{})
	require.NoError(t, err)

	changeItemBuilder := helpers.NewChangeItemsBuilder("public", "", arrColSchema)
	item := changeItemBuilder.Inserts(t, []map[string]interface{}{{"int": 1, "text": 1, "val": 2}})[0]

	result, err := dblog.PKeysToStringArr(&item, []string{"int", "text"}, converter)
	require.NoError(t, err)
	require.Equal(t, []string{"'1'", "'1'"}, result)
}

func TestResolvePrimaryKeyColumns(t *testing.T) {
	t.Run("supported primary key", func(t *testing.T) {
		result, err := dblog.ResolvePrimaryKeyColumns(context.TODO(), storage, abstract.TableID{}, isSupporterKeyType)
		require.NoError(t, err)
		require.Equal(t, []string{"int", "text"}, result)
	})

	t.Run("unsupported primary key", func(t *testing.T) {
		_, err := dblog.ResolvePrimaryKeyColumns(context.TODO(), storage, abstract.TableID{Name: "uncorrectPk"}, isSupporterKeyType)
		require.Error(t, err)
	})

	t.Run("table without primary key", func(t *testing.T) {
		_, err := dblog.ResolvePrimaryKeyColumns(context.TODO(), storage, abstract.TableID{Name: "tableWithoutPk"}, isSupporterKeyType)
		require.Error(t, err)
	})
}

func TestResolveChunkMapFromArr(t *testing.T) {
	arrColSchema, err := storage.TableSchema(context.TODO(), abstract.TableID{})
	require.NoError(t, err)

	changeItemBuilder := helpers.NewChangeItemsBuilder("public", "", arrColSchema)
	items := changeItemBuilder.Inserts(t, []map[string]interface{}{{"int": 1, "text": 1, "val": 2}, {"int": 11, "text": 1, "val": 11}})

	expected := map[string]abstract.ChangeItem{
		"3#'1'3#'1'":  items[0],
		"4#'11'3#'1'": items[1],
	}

	result, err := dblog.ResolveChunkMapFromArr(items, []string{"int", "text"}, converter)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestConvertArrayToString(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		want    string
		wantErr bool
	}{
		{
			name:    "Normal case",
			input:   []string{"раз", "два", "три"},
			want:    `["раз","два","три"]`,
			wantErr: false,
		},
		{
			name:    "Empty array",
			input:   []string{},
			want:    `[]`,
			wantErr: false,
		},
		{
			name:    "Array with one element",
			input:   []string{"раз"},
			want:    `["раз"]`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dblog.ConvertArrayToString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertArrayToString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ConvertArrayToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConvertStringToArray(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr bool
	}{
		{
			name:    "Normal case",
			input:   `["раз","два","три"]`,
			want:    []string{"раз", "два", "три"},
			wantErr: false,
		},
		{
			name:    "Empty array",
			input:   `[]`,
			want:    []string{},
			wantErr: false,
		},
		{
			name:    "Array with one element",
			input:   `["раз"]`,
			want:    []string{"раз"},
			wantErr: false,
		},
		{
			name:    "Invalid JSON",
			input:   `["раз","два","три"`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dblog.ConvertStringToArray(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertStringToArray() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConvertStringToArray() = %v, want %v", got, tt.want)
			}
		})
	}
}

func CreateMockStorage() *mockstorage.MockStorage {
	currStorage := mockstorage.NewMockStorage()
	currStorage.ExactTableRowsCountF = func(table abstract.TableID) (uint64, error) {
		return 1, nil
	}
	currStorage.EstimateTableRowsCountF = func(table abstract.TableID) (uint64, error) {
		if table.Name == "nullRowsCount" {
			return 0, nil
		}

		return 1, nil
	}
	currStorage.TableSizeInBytesF = func(table abstract.TableID) (uint64, error) {
		if table.Name == "nullTableSize" {
			return 0, nil
		}

		return 1, nil
	}
	currStorage.TableSchemaF = func(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
		var arrColSchema *abstract.TableSchema

		switch table.Name {
		case "tableWithoutPk":
			arrColSchema = abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "int", DataType: ytschema.TypeInt32.String(), PrimaryKey: false},
			})
		case "uncorrectPk":
			arrColSchema = abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "uncorrectPk", DataType: "uncorrectType", PrimaryKey: true, OriginalType: "uncorrectType"},
			})
		default:
			arrColSchema = abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "int", DataType: ytschema.TypeInt32.String(), PrimaryKey: true, OriginalType: "pg:int"},
				{ColumnName: "text", DataType: ytschema.TypeString.String(), PrimaryKey: true, OriginalType: "pg:text"},
				{ColumnName: "val", DataType: ytschema.TypeInt32.String(), PrimaryKey: false, OriginalType: "pg:int"},
			})
		}

		return arrColSchema, nil
	}

	return currStorage
}
