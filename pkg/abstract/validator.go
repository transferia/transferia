package abstract

import (
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

func ValidateChangeItem(changeItem *ChangeItem) error {
	if changeItem.Kind != UpdateKind && changeItem.Kind != InsertKind && changeItem.Kind != DeleteKind {
		return nil
	}
	if len(changeItem.ColumnNames) != len(changeItem.ColumnValues) {
		malformedChangeItemJSON, _ := json.Marshal(changeItem)
		return xerrors.Errorf("len(changeItem.ColumnNames) != len(changeItem.ColumnValues): %s", malformedChangeItemJSON)
	}
	return nil
}

func ValidateChangeItemsPtrs(changeItems []*ChangeItem) error {
	if changeItems == nil {
		return nil
	}
	for _, changeItem := range changeItems {
		if err := ValidateChangeItem(changeItem); err != nil {
			return err
		}
	}
	return nil
}

func ValidateChangeItems(changeItems []ChangeItem) error {
	for _, changeItem := range changeItems {
		if err := ValidateChangeItem(&changeItem); err != nil {
			return err
		}
	}
	return nil
}
