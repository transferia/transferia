package provider

import (
	"reflect"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
	"go.ytsaurus.tech/yt/go/yt"
)

// rowDecoder encapsulates both decode paths:
// - struct-based decode for primitive-only schemas
// - map-based decode for schemas with complex YT types
type rowDecoder struct {
	useMapDecode bool

	rowType   reflect.Type
	rowPtr    reflect.Value
	converter rowConverter

	sizeEstimator *rowSizeEstimator

	idxColName string
	cols       []yt_table.YtColumn
}

func newRowDecoder(tbl yt_table.YtTable, idxColName string) *rowDecoder {
	if tableHasComplexColumns(tbl, idxColName) {
		cols := make([]yt_table.YtColumn, tbl.ColumnsCount())
		for i := 0; i < tbl.ColumnsCount(); i++ {
			cols[i] = tbl.Column(i).(yt_table.YtColumn)
		}
		return &rowDecoder{
			useMapDecode:  true,
			rowType:       nil,
			rowPtr:        reflect.Value{},
			converter:     nil,
			sizeEstimator: makeRowSizeEstimator(tbl, idxColName),
			idxColName:    idxColName,
			cols:          cols,
		}
	}
	return &rowDecoder{
		useMapDecode:  false,
		rowType:       buildSkiffRowType(tbl, idxColName),
		rowPtr:        reflect.Value{},
		converter:     makeRowConverter(tbl, idxColName),
		sizeEstimator: makeRowSizeEstimator(tbl, idxColName),
		idxColName:    "",
		cols:          nil,
	}
}

func (d *rowDecoder) cloneForReader() *rowDecoder {
	if d == nil {
		return nil
	}
	cloned := *d
	if !cloned.useMapDecode && cloned.rowType != nil {
		cloned.rowPtr = reflect.New(cloned.rowType)
	}
	return &cloned
}

func (d *rowDecoder) decode(reader yt.TableReader, rowIdx uint64) ([]interface{}, error) {
	if d.useMapDecode {
		rowMap := map[string]any{}
		if err := reader.Scan(&rowMap); err != nil {
			return nil, xerrors.Errorf("scan error: %w", err)
		}
		values, err := mapRowToValues(rowMap, int64(rowIdx), d.cols, d.idxColName)
		if err != nil {
			return nil, xerrors.Errorf("convert row %d error: %w", rowIdx, err)
		}
		return values, nil
	}

	if err := reader.Scan(d.rowPtr.Interface()); err != nil {
		return nil, xerrors.Errorf("scan error: %w", err)
	}
	values, err := d.converter(d.rowPtr.Elem(), int64(rowIdx))
	if err != nil {
		return nil, xerrors.Errorf("convert row %d error: %w", rowIdx, err)
	}
	return values, nil
}
