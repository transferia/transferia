package sink

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type deleteRowsFn = func(ctx context.Context, tx yt.TabletTx, tablePath ypath.Path, keys []interface{}) error
type ytRow = map[columnName]interface{}

type ytDataBatch struct {
	toUpdateKeys  []interface{}
	toUpdateRows  []ytRow
	toInsert      []interface{}
	toDelete      []interface{}
	insertOptions yt.InsertRowsOptions
	deleteRows    deleteRowsFn
}

func (b *ytDataBatch) addUpdate(item changeItemView) error {
	isKeysChanged, err := item.keysChanged()
	if err != nil {
		return xerrors.Errorf("Cannot check if keys were changed: %w", err)
	}
	if !isKeysChanged {
		//nolint:descriptiveerrors
		return b.addInsert(item)
	}

	key, err := item.makeOldKeys()
	if err != nil {
		return xerrors.Errorf("Cannot create old keys: %w", err)
	}
	b.toUpdateKeys = append(b.toUpdateKeys, key)

	row, err := item.makeRow()
	if err != nil {
		return xerrors.Errorf("Cannot create column values: %w", err)
	}
	b.toUpdateRows = append(b.toUpdateRows, row)

	return nil
}

func (b *ytDataBatch) addInsert(item changeItemView) error {
	row, err := item.makeRow()
	if err != nil {
		return xerrors.Errorf("Cannot create column values: %w", err)
	}
	b.toInsert = append(b.toInsert, row)
	return nil
}

func (b *ytDataBatch) addDelete(item changeItemView) error {
	row, err := item.makeOldKeys()
	if err != nil {
		return xerrors.Errorf("Cannot create old keys: %w", err)
	}
	b.toDelete = append(b.toDelete, row)
	return nil
}

func (b *ytDataBatch) process(ctx context.Context, tx yt.TabletTx, tablePath ypath.Path) error {
	if len(b.toUpdateKeys) > 0 { // Handle primary key updates, TM-1143
		reader, err := tx.LookupRows(ctx, tablePath, b.toUpdateKeys, &yt.LookupRowsOptions{KeepMissingRows: true})
		if err != nil {
			return xerrors.Errorf("Cannot lookup %d rows: %w", len(b.toUpdateKeys), err)
		}
		defer reader.Close()

		i := 0
		for reader.Next() {
			var oldRow ytRow
			if err := reader.Scan(&oldRow); err != nil {
				return xerrors.Errorf("Cannot scan value: %w", err)
			}
			if i > len(b.toUpdateRows) {
				return xerrors.Errorf("Table lookup returned extra rows")
			}
			if oldRow == nil {
				b.toInsert = append(b.toInsert, b.toUpdateRows[i])
			} else {
				updatedRow := oldRow
				for colName, colValue := range b.toUpdateRows[i] {
					updatedRow[colName] = colValue
				}
				b.toInsert = append(b.toInsert, updatedRow)
				b.toDelete = append(b.toDelete, b.toUpdateKeys[i])
			}
			i++
		}
		if reader.Err() != nil {
			return xerrors.Errorf("Cannot read value: %w", err)
		}
		if i != len(b.toUpdateKeys) {
			return xerrors.Errorf("Table lookup returned insufficient amount of rows")
		}
	}
	if len(b.toInsert) > 0 {
		if err := tx.InsertRows(ctx, tablePath, b.toInsert, &b.insertOptions); err != nil {
			return xerrors.Errorf("Cannot insert %d rows: %w", len(b.toInsert), err)
		}
	}
	if len(b.toDelete) > 0 {
		if err := b.deleteRows(ctx, tx, tablePath, b.toDelete); err != nil {
			return xerrors.Errorf("Cannot delete %d rows: %w", len(b.toDelete), err)
		}
	}
	return nil
}
