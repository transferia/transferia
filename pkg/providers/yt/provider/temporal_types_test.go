package provider

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestExtendedTemporalTypesSnapshot(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	ctx := context.Background()

	// YT's temporal bounds, in days, seconds and microseconds respectively.
	const minDay int64 = -53375809
	const maxDay int64 = 53375807
	const minSecond = minDay * 86400
	const maxSecond = (maxDay+1)*86400 - 1
	temporalColumns := []struct {
		name       string
		ytType     ytschema.Type
		targetType ytschema.Type
		min        int64
		max        int64
	}{
		{"day", ytschema.TypeDate32, ytschema.TypeInt32, minDay, maxDay},
		{"second", ytschema.TypeDatetime64, ytschema.TypeInt64, minSecond, maxSecond},
		{"microsecond", ytschema.TypeTimestamp64, ytschema.TypeInt64, minSecond * 1000000, maxSecond*1000000 + 999999},
		{"duration", ytschema.TypeInterval64, ytschema.TypeInt64, -9223339708800000000, 9223339708800000000},
	}

	for _, mode := range []string{"primitive", "with_complex_column"} {
		t.Run(mode, func(t *testing.T) {
			tablePath := env.TmpPath()
			columns := []ytschema.Column{{Name: "id", ComplexType: ytschema.TypeInt64}}
			for _, col := range temporalColumns {
				columns = append(columns,
					ytschema.Column{Name: col.name, ComplexType: col.ytType},
					ytschema.Column{Name: "optional_" + col.name, ComplexType: ytschema.Optional{Item: col.ytType}},
				)
			}
			if mode == "with_complex_column" {
				// Any composite column selects the map decoder for the whole row.
				columns = append(columns, ytschema.Column{Name: "list", ComplexType: ytschema.List{Item: ytschema.TypeInt64}})
			}
			_, err := env.YT.CreateNode(ctx, tablePath, yt.NodeTable, &yt.CreateNodeOptions{
				Attributes: map[string]any{"schema": ytschema.Schema{Columns: columns}},
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, env.YT.RemoveNode(ctx, tablePath, nil)) }()

			writer, err := yt.WriteTable(ctx, env.YT, tablePath, yt.WithExistingTable())
			require.NoError(t, err)
			defer func() { _ = writer.Rollback() }()
			expectedRows := make([]map[string]any, 6)
			for rowIndex := range expectedRows {
				row := map[string]any{"id": int64(rowIndex)}
				expected := map[string]any{"id": int64(rowIndex)}
				for _, col := range temporalColumns {
					raw := []int64{col.min, -1, 0, 1, col.max, 0}[rowIndex]
					var value any = raw
					if col.targetType == ytschema.TypeInt32 {
						value = int32(raw)
					}
					row[col.name], expected[col.name] = raw, value
					if rowIndex == len(expectedRows)-1 {
						row["optional_"+col.name], expected["optional_"+col.name] = nil, nil
					} else {
						row["optional_"+col.name], expected["optional_"+col.name] = raw, value
					}
				}
				if mode == "with_complex_column" {
					row["list"] = []any{int64(7)}
				}
				require.NoError(t, writer.Write(row))
				expectedRows[rowIndex] = expected
			}
			require.NoError(t, writer.Commit())

			// Verify the fixture actually contains the new YT types.
			var storedSchema ytschema.Schema
			require.NoError(t, env.YT.GetNode(ctx, tablePath.Attr("schema"), &storedSchema, nil))
			require.Equal(t, columns, storedSchema.Normalize().Columns)

			src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), testYtSourceCfg(tablePath), nil)
			require.NoError(t, err)
			defer src.Close()
			tables, err := src.TableList(nil)
			require.NoError(t, err)
			require.Len(t, tables, 1)
			for tableID, tableInfo := range tables {
				sch, err := src.TableSchema(ctx, tableID)
				require.NoError(t, err)
				require.Equal(t, tableInfo.Schema.Columns(), sch.Columns())
				require.Len(t, sch.Columns(), len(columns))
				for _, col := range temporalColumns {
					for _, name := range []string{col.name, "optional_" + col.name} {
						column := sch.FastColumns()[abstract.ColumnName(name)]
						require.Equal(t, col.targetType.String(), column.DataType)
						require.Equal(t, name == col.name, column.Required)
						require.Empty(t, column.OriginalType)
						require.Empty(t, column.Properties)
					}
				}

				seen := make(map[int64]bool)
				err = src.LoadTable(ctx, abstract.TableDescription{Name: tableID.Name, Schema: tableID.Namespace}, func(items []abstract.ChangeItem) error {
					for _, item := range items {
						require.Equal(t, abstract.InsertKind, item.Kind)
						require.Equal(t, sch.Columns(), item.TableSchema.Columns())
						indices := item.ColumnNameIndices()
						rowID := item.ColumnValues[indices["id"]].(int64)
						require.False(t, seen[rowID], "duplicate row %d", rowID)
						seen[rowID] = true
						for name, expected := range expectedRows[rowID] {
							require.Equal(t, expected, item.ColumnValues[indices[name]], "row %d, column %s", rowID, name)
						}

						// The source must already emit the exact types required by Strictify.
						values := append([]any(nil), item.ColumnValues...)
						item.ColumnValues = append([]any(nil), item.ColumnValues...)
						require.NoError(t, strictify.Strictify(&item, sch.FastColumns()))
						require.Equal(t, values, item.ColumnValues)
					}
					return nil
				})
				require.NoError(t, err)
				require.Len(t, seen, len(expectedRows))
			}
		})
	}
}
