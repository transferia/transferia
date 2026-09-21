package canon

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/jsonx"
)

func CanonizeTableChangeItems(t *testing.T, storage abstract.Storage, table abstract.TableDescription) {
	result := make([]abstract.ChangeItem, 0)
	err := storage.LoadTable(context.Background(), table, func(input []abstract.ChangeItem) error {
		result = append(result, input...)
		return nil
	})
	require.NoError(t, err)
	for i := range result {
		result[i].CommitTime = 0
	}
	canon.SaveJSON(t, result)
}

func AddIndentToJSON(t *testing.T, jsonStr string) string {
	var obj any
	require.NoError(t, jsonx.Unmarshal([]byte(jsonStr), &obj))
	res, err := json.MarshalIndent(obj, "", "  ")
	require.NoError(t, err)
	return string(res)
}
