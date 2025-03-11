package helpers

import (
	"fmt"
	"testing"

	"github.com/transferia/transferia/pkg/abstract"
)

func UnmarshalChangeItems(t *testing.T, changeItemBuf []byte) []abstract.ChangeItem {
	result, err := abstract.UnmarshalChangeItems(changeItemBuf)
	if err != nil {
		t.FailNow()
	}
	return result
}

func UnmarshalChangeItemsStr(t *testing.T, in string) []abstract.ChangeItem {
	return UnmarshalChangeItems(t, []byte(in))
}

func UnmarshalChangeItem(t *testing.T, changeItemBuf []byte) *abstract.ChangeItem {
	result, err := abstract.UnmarshalChangeItem(changeItemBuf)
	if err != nil {
		t.FailNow()
	}
	return result
}

func UnmarshalChangeItemStr(t *testing.T, in string) *abstract.ChangeItem {
	return UnmarshalChangeItem(t, []byte(in))
}

func TableIDFullName(tableID abstract.TableID) string {
	return fmt.Sprintf(`"%s"."%s"`, tableID.Namespace, tableID.Name)
}
