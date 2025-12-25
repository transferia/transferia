package changeitem

import (
	"encoding/json"
	"os"
	"sort"

	"github.com/olekukonko/tablewriter"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
)

func Dump(input []ChangeItem) {
	tablets := map[string][]ChangeItem{}
	for _, c := range input {
		tablets[c.Fqtn()] = append(tablets[c.Fqtn()], c)
	}
	for _, rows := range tablets {
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeaderLine(true)
		table.SetRowLine(true)
		cols := map[string]bool{}
		for _, row := range rows {
			for _, col := range row.ColumnNames {
				cols[col] = true
			}
		}
		var colNames []string
		for col := range cols {
			colNames = append(colNames, col)
		}
		sort.Strings(colNames)
		colIdx := map[string]int{}
		for i, col := range colNames {
			colIdx[col] = i
		}
		table.SetHeader(colNames)
		for _, row := range rows {
			switch row.Kind {
			case InsertKind, UpdateKind, DeleteKind, MongoUpdateDocumentKind:
				data := make([]string, len(colNames))
				for i, v := range row.ColumnValues {
					switch vv := v.(type) {
					case []byte:
						data[colIdx[row.ColumnNames[i]]] = util.Sample(string(vv), 100)
					case schema.Datetime:
						data[colIdx[row.ColumnNames[i]]] = vv.Time().String()
					default:
						d, _ := json.MarshalIndent(v, "", "	")
						data[colIdx[row.ColumnNames[i]]] = util.Sample(string(d), 100)
					}
				}
				table.Append(data)
			}
		}
		table.Render()
	}
}
