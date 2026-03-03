package abstract

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	runnableInterfaceType = reflect.TypeOf((*RunnableTask)(nil)).Elem()
	fakeInterfaceType     = reflect.TypeOf((*FakeTask)(nil)).Elem()
)

func TestRunnableTasks(t *testing.T) {
	require.Greater(t, len(RunnableTasks), 0)
	names := map[string]struct{}{}
	for _, task := range RunnableTasks {
		require.True(t, reflect.TypeOf(task).Implements(runnableInterfaceType))
		require.False(t, reflect.TypeOf(task).Implements(fakeInterfaceType))
		typeName := reflect.TypeOf(task).Name()
		names[typeName] = struct{}{}
	}
	require.EqualValues(t, len(names), len(RunnableTasks))
}

func TestFakeTasks(t *testing.T) {
	require.Greater(t, len(FakeTasks), 0)
	names := map[string]struct{}{}
	for _, task := range FakeTasks {
		require.True(t, reflect.TypeOf(task).Implements(fakeInterfaceType))
		require.False(t, reflect.TypeOf(task).Implements(runnableInterfaceType))
		typeName := reflect.TypeOf(task).Name()
		names[typeName] = struct{}{}
	}
	require.EqualValues(t, len(names), len(FakeTasks))
}

func TestAllTasks(t *testing.T) {
	require.Greater(t, len(AllTasks), 0)
	require.EqualValues(t, len(RunnableTasks)+len(FakeTasks), len(AllTasks))
}

func TestUpdateTransfer_AddedTables(t *testing.T) {
	var params UpdateTransferParams
	require.NoError(t, json.Unmarshal([]byte(`{
  "old_objects": [
    "public.orders",
    "\"public\".\"orders_log\"",
    "\"public\".\"stocks\"",
    "\"public\".\"courier_shifts\"",
    "\"public\".\"stocks_log\"",
    "public.couriers",
    "public.products",
    "public.product_groups",
    "public.wallets_log",
    "public.suggests"
  ],
  "new_objects": [
    "\"public\".\"orders\"",
    "\"public\".\"orders2\"",
    "\"public\".\"orders_log\"",
    "\"public\".\"stocks\"",
    "\"public\".\"courier_shifts\"",
    "\"public\".\"stocks_log\"",
    "public.couriers",
    "public.products",
    "public.product_groups",
    "public.wallets_log",
    "public.suggests"
  ]
}`), &params))
	params.ProviderType = ProviderType("pg")
	added, err := params.AddedTables()
	require.NoError(t, err)
	require.Len(t, added, 1)
}
