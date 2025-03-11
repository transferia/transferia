package validator

import (
	"fmt"
	"strings"
	"testing"

	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"golang.org/x/exp/slices"
)

type ReferencerSink struct {
	rows                  []abstract.ChangeItem
	t                     *testing.T
	changeItemMiddlewares []func(items []abstract.ChangeItem) []abstract.ChangeItem
}

type Container struct {
	GoType string
	Val    interface{}
}

type ReferenceRow struct {
	Data map[string]Container
}

type ReferenceTable struct {
	TableID     abstract.TableID
	Rows        []ReferenceRow
	TableSchema abstract.TableColumns
}

func (c *ReferencerSink) Close() error {
	for _, mw := range c.changeItemMiddlewares {
		c.rows = mw(c.rows)
	}
	data := map[abstract.TableID][]ReferenceRow{}
	schemas := map[abstract.TableID]abstract.TableColumns{}
	for _, row := range c.rows {
		data[row.TableID()] = append(data[row.TableID()], c.AsReferenceRow(row))
		schemas[row.TableID()] = row.TableSchema.Columns()
	}
	var canonRes []ReferenceTable
	for tid, rows := range data {
		canonRes = append(canonRes, ReferenceTable{
			TableID:     tid,
			Rows:        rows,
			TableSchema: schemas[tid],
		})
	}
	slices.SortFunc(canonRes, func(a, b ReferenceTable) int {
		return strings.Compare(a.TableID.Fqtn(), b.TableID.Fqtn())
	})
	for _, rt := range canonRes {
		c.t.Run(fmt.Sprintf("%s.%s", rt.TableID.Namespace, rt.TableID.Name), func(t *testing.T) {
			canon.SaveJSON(t, rt)
		})
	}
	return nil
}

func (c *ReferencerSink) Push(items []abstract.ChangeItem) error {
	for _, row := range items {
		if !row.IsRowEvent() || row.IsSystemTable() {
			logger.Log.Info("non-row event presented")
			continue
		}
		c.rows = append(c.rows, row)
	}
	return nil
}

func (c *ReferencerSink) AsReferenceRow(row abstract.ChangeItem) ReferenceRow {
	res := &ReferenceRow{
		Data: map[string]Container{},
	}
	for i, colName := range row.ColumnNames {
		res.Data[colName] = Container{
			GoType: fmt.Sprintf("%T", row.ColumnValues[i]),
			Val:    row.ColumnValues[i],
		}
	}
	return *res
}

func Referencer(
	t *testing.T,
	changeItemMiddlewares ...func(item []abstract.ChangeItem) []abstract.ChangeItem,
) func() abstract.Sinker {
	return func() abstract.Sinker {
		return &ReferencerSink{
			t:                     t,
			changeItemMiddlewares: append(changeItemMiddlewares, RemoveVariableFieldsRowMiddleware),
		}
	}
}
