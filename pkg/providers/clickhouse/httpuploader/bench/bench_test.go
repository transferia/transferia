//go:build !disable_clickhouse_provider

package bench

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpuploader"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/canon"
)

var tables = map[changeitem.TableID][]changeitem.ChangeItem{}

func init() {
	for _, tc := range canon.All(postgres.ProviderType) {
		for _, row := range tc.Data {
			tables[row.TableID()] = append(tables[row.TableID()], row)
		}
	}
	for _, tc := range canon.All(mysql.ProviderType) {
		for _, row := range tc.Data {
			tables[row.TableID()] = append(tables[row.TableID()], row)
		}
	}
}

func BenchmarkMarshalCIFromDB(b *testing.B) {
	for tid := range tables {
		b.Run(tid.Fqtn(), func(b *testing.B) {
			marshalBench(b, tables[tid])
		})
	}
}

func marshalBench(b *testing.B, data []changeitem.ChangeItem) {
	idx := map[string]int{}
	if len(data) == 0 {
		return
	}
	for i, k := range data[0].ColumnNames {
		idx[k] = i
	}
	colTypes := map[string]*columntypes.TypeDescription{}
	for _, v := range data[0].ColumnNames {
		colTypes[v] = new(columntypes.TypeDescription)
	}
	rules := httpuploader.NewRules(data[0].ColumnNames, data[0].TableSchema.Columns(), idx, colTypes, false)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		for _, r := range data {
			require.NoError(b, httpuploader.MarshalCItoJSON(r, rules, buf))
		}
		b.SetBytes(int64(buf.Len()))
	}
	b.ReportAllocs()
}
