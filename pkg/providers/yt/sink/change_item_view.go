// Used only in sorted_table
package sink

import (
	"reflect"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
)

type changeItemView interface {
	keysChanged() (bool, error)
	makeOldKeys() (ytRow, error)
	makeRow() (ytRow, error)
}

type dataItemView struct {
	change  *abstract.ChangeItem
	columns *tableColumns
}

func (di *dataItemView) keysChanged() (bool, error) {
	return di.change.KeysChanged(), nil
}

func (di *dataItemView) makeOldKeys() (ytRow, error) {
	row := ytRow{}
	for i, colName := range di.change.OldKeys.KeyNames {
		tableColumn, ok := di.columns.getByName(colName)
		if !ok {
			return nil, xerrors.Errorf("Cannot find column %s in schema %v", colName, di.columns.columns)
		}
		if tableColumn.PrimaryKey {
			var err error
			row[colName], err = Restore(tableColumn, di.change.OldKeys.KeyValues[i])
			if err != nil {
				return nil, xerrors.Errorf("Cannot restore value for column '%s': %w", colName, err)
			}
		}
	}
	if len(row) == 0 {
		return nil, xerrors.Errorf("No old key columns found for change item %s", util.Sample(di.change.ToJSONString(), 10000))
	}
	return row, nil
}

func (di *dataItemView) makeRow() (ytRow, error) {
	row := ytRow{}
	for i, colName := range di.change.ColumnNames {
		tableColumn, ok := di.columns.getByName(colName)
		if !ok {
			return nil, xerrors.Errorf("Cannot find column %s in schema %v", colName, di.columns.columns)
		}
		var err error
		row[colName], err = Restore(tableColumn, di.change.ColumnValues[i])
		if err != nil {
			return nil, xerrors.Errorf("Cannot restore value for column '%s': %w", colName, err)
		}
	}
	if di.columns.hasOnlyPKey() {
		row["__dummy"] = nil
	}
	return row, nil
}

func newDataItemView(change *abstract.ChangeItem, columns *tableColumns) dataItemView {
	return dataItemView{change: change, columns: columns}
}

type indexItemView struct {
	dataView        dataItemView
	change          *abstract.ChangeItem
	oldRow          ytRow
	columns         *tableColumns
	indexColumnPos  int
	indexColumnName string
}

func (ii *indexItemView) indexColumnChanged() (bool, error) {
	if ii.change.Kind != "update" || ii.oldRow == nil {
		return false, nil
	}
	indexTableColumn, ok := ii.columns.getByName(ii.indexColumnName)
	if !ok || ii.indexColumnPos < 0 {
		return false, nil
	}
	newIndexValue, err := Restore(indexTableColumn, ii.change.ColumnValues[ii.indexColumnPos])
	if err != nil {
		return false, xerrors.Errorf("Cannot restore value for index column '%s': %w", ii.indexColumnName, err)
	}

	oldIndexValue, ok := ii.oldRow[ii.indexColumnName]
	if !ok {
		return false, nil
	}

	return !reflect.DeepEqual(oldIndexValue, newIndexValue), nil
}

func (ii *indexItemView) keysChanged() (bool, error) {
	isIndexColumnChanged, err := ii.indexColumnChanged()
	if err != nil {
		return false, xerrors.Errorf("Cannot check if index column changed: %w", err)
	}
	isKeysChanged, err := ii.dataView.keysChanged()
	if err != nil {
		return false, xerrors.Errorf("Cannot check if keys changed: %w", err)
	}
	return isIndexColumnChanged || isKeysChanged, nil
}

func (ii *indexItemView) makeOldKeys() (ytRow, error) {
	dataKeys, err := ii.dataView.makeOldKeys()
	if err != nil {
		return nil, err
	}
	oldKeys := ytRow{ii.indexColumnName: ii.oldRow[ii.indexColumnName]}
	for key, value := range dataKeys {
		oldKeys[key] = value
	}
	return oldKeys, nil
}

func (ii *indexItemView) makeRow() (ytRow, error) {
	tableColumn, ok := ii.columns.getByName(ii.indexColumnName)
	if !ok {
		return nil, xerrors.Errorf("Cannot find column %s in schema %v", ii.indexColumnName, ii.columns.columns)
	}

	value, err := Restore(tableColumn, ii.change.ColumnValues[ii.indexColumnPos])
	if err != nil {
		return nil, xerrors.Errorf("Cannot restore value for index column '%s': %w", tableColumn.ColumnName, err)
	}
	row := ytRow{
		ii.indexColumnName: value,
		"_dummy":           nil,
	}

	for i, colName := range ii.change.ColumnNames {
		tableColumn, ok := ii.columns.getByName(colName)
		if !ok {
			return nil, xerrors.Errorf("Cannot find column %s in schema %v", ii.indexColumnName, ii.columns.columns)
		}
		if !tableColumn.IsKey() {
			continue
		}

		row[colName], err = Restore(tableColumn, ii.change.ColumnValues[i])
		if err != nil {
			return nil, xerrors.Errorf("Cannot restore value for column '%s': %w", colName, err)
		}
	}
	return row, nil
}

var noIndexColumn error = xerrors.New("Index column not found")

func newIndexItemView(change *abstract.ChangeItem, columns *tableColumns, indexColName columnName, oldRow ytRow) (indexItemView, error) {
	dataView := newDataItemView(change, columns)

	if _, ok := columns.getByName(indexColName); !ok {
		return indexItemView{}, noIndexColumn
	}

	indexColumnPos := -1
	for i, colName := range change.ColumnNames {
		if colName == indexColName {
			indexColumnPos = i
			break
		}
	}

	return indexItemView{
		dataView:        dataView,
		change:          change,
		oldRow:          oldRow,
		columns:         columns,
		indexColumnPos:  indexColumnPos,
		indexColumnName: indexColName,
	}, nil
}
