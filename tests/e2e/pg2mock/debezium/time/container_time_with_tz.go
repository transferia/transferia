package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/yatest"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	"github.com/transferia/transferia/pkg/debezium/testutil"
	"github.com/transferia/transferia/tests/helpers"
)

func check(t *testing.T, changeItem abstract.ChangeItem, key []byte, val string, isSnapshot bool) {
	testutil.CheckCanonizedDebeziumEvent(t, &changeItem, "fullfillment", "pguser", "pg", isSnapshot, []debeziumcommon.KeyValue{{DebeziumKey: string(key), DebeziumVal: &val}})
	changeItemBuf, err := json.Marshal(changeItem)
	require.NoError(t, err)
	changeItemDeserialized := helpers.UnmarshalChangeItem(t, changeItemBuf)
	testutil.CheckCanonizedDebeziumEvent(t, changeItemDeserialized, "fullfillment", "pguser", "pg", isSnapshot, []debeziumcommon.KeyValue{{DebeziumKey: string(key), DebeziumVal: &val}})
}

var insertStmt0 = `
INSERT INTO basic_types (id, t_timestamp_without_tz, t_timestamp_with_tz, t_time_without_tz, t_time_with_tz, t_interval) VALUES (
    3,
    '2022-08-28 19:49:47.749906',
    '2022-08-28 19:49:47.749906 +00:00',
    '19:49:47.749906',
    '19:49:47.749906 +00:00',
    '1 year 2 months 3 days 4 hours 5 minutes 6 seconds 7 microseconds'
);
`

var insertStmt1 = `
INSERT INTO basic_types (id, t_timestamp_without_tz, t_timestamp_with_tz, t_time_without_tz, t_time_with_tz, t_interval) VALUES (
    4,
    '2022-08-28 19:49:47.74990',
    '2022-08-28 19:49:47.74990 +00:00',
    '19:49:47.74990',
    '19:49:47.74990 +00:00',
    '1 year 2 months 3 days 4 hours 5 minutes 6 seconds 70 microseconds'
);
`

type containerTimeWithTZ struct {
	canonizedDebeziumVal0 string
	canonizedDebeziumVal1 string
	canonizedDebeziumVal2 string
	canonizedDebeziumVal3 string

	canonizedDebeziumKeyBytes0 []byte
	canonizedDebeziumKeyBytes1 []byte
	canonizedDebeziumKeyBytes2 []byte
	canonizedDebeziumKeyBytes3 []byte

	changeItems []abstract.ChangeItem
}

func (c *containerTimeWithTZ) TableName() string {
	return "basic_types"
}

func (c *containerTimeWithTZ) Initialize(t *testing.T) {
	var err error

	c.canonizedDebeziumKeyBytes0, err = os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2mock/debezium/time/testdata/change_item_key_0.txt"))
	require.NoError(t, err)
	canonizedDebeziumValBytes0, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2mock/debezium/time/testdata/change_item_val_0.txt"))
	require.NoError(t, err)
	c.canonizedDebeziumVal0 = string(canonizedDebeziumValBytes0)

	c.canonizedDebeziumKeyBytes1, err = os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2mock/debezium/time/testdata/change_item_key_1.txt"))
	require.NoError(t, err)
	canonizedDebeziumValBytes1, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2mock/debezium/time/testdata/change_item_val_1.txt"))
	require.NoError(t, err)
	c.canonizedDebeziumVal1 = string(canonizedDebeziumValBytes1)

	c.canonizedDebeziumKeyBytes2, err = os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2mock/debezium/time/testdata/change_item_key_2.txt"))
	require.NoError(t, err)
	canonizedDebeziumValBytes2, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2mock/debezium/time/testdata/change_item_val_2.txt"))
	require.NoError(t, err)
	c.canonizedDebeziumVal2 = string(canonizedDebeziumValBytes2)

	c.canonizedDebeziumKeyBytes3, err = os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2mock/debezium/time/testdata/change_item_key_3.txt"))
	require.NoError(t, err)
	canonizedDebeziumValBytes3, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2mock/debezium/time/testdata/change_item_val_3.txt"))
	require.NoError(t, err)
	c.canonizedDebeziumVal3 = string(canonizedDebeziumValBytes3)
}

func (c *containerTimeWithTZ) ExecStatement(ctx context.Context, t *testing.T, client *pgxpool.Pool) {
	var err error
	_, err = client.Exec(ctx, insertStmt0)
	require.NoError(t, err)
	_, err = client.Exec(ctx, insertStmt1)
	require.NoError(t, err)
}

func (c *containerTimeWithTZ) AddChangeItem(in *abstract.ChangeItem) {
	c.changeItems = append(c.changeItems, *in)
}

func (c *containerTimeWithTZ) IsEnoughChangeItems() bool {
	return len(c.changeItems) == 8
}

func (c *containerTimeWithTZ) Check(t *testing.T) {
	require.Equal(t, 8, len(c.changeItems))
	require.Equal(t, c.changeItems[0].Kind, abstract.InitShardedTableLoad)
	require.Equal(t, c.changeItems[1].Kind, abstract.InitTableLoad)
	require.Equal(t, c.changeItems[2].Kind, abstract.InsertKind)
	require.Equal(t, c.changeItems[3].Kind, abstract.InsertKind)
	require.Equal(t, c.changeItems[4].Kind, abstract.DoneTableLoad)
	require.Equal(t, c.changeItems[5].Kind, abstract.DoneShardedTableLoad)
	require.Equal(t, c.changeItems[6].Kind, abstract.InsertKind)
	require.Equal(t, c.changeItems[7].Kind, abstract.InsertKind)

	fmt.Printf("changeItem dump: %s\n", c.changeItems[2].ToJSONString())
	fmt.Printf("changeItem dump: %s\n", c.changeItems[3].ToJSONString())
	fmt.Printf("changeItem dump: %s\n", c.changeItems[6].ToJSONString())
	fmt.Printf("changeItem dump: %s\n", c.changeItems[7].ToJSONString())

	check(t, c.changeItems[2], c.canonizedDebeziumKeyBytes0, c.canonizedDebeziumVal0, true)
	check(t, c.changeItems[3], c.canonizedDebeziumKeyBytes1, c.canonizedDebeziumVal1, true)
	check(t, c.changeItems[6], c.canonizedDebeziumKeyBytes2, c.canonizedDebeziumVal2, false)
	check(t, c.changeItems[7], c.canonizedDebeziumKeyBytes3, c.canonizedDebeziumVal3, false)
}

func newContainerTimeWithTZ() *containerTimeWithTZ {
	return &containerTimeWithTZ{
		canonizedDebeziumVal0: "",
		canonizedDebeziumVal1: "",
		canonizedDebeziumVal2: "",
		canonizedDebeziumVal3: "",

		canonizedDebeziumKeyBytes0: nil,
		canonizedDebeziumKeyBytes1: nil,
		canonizedDebeziumKeyBytes2: nil,
		canonizedDebeziumKeyBytes3: nil,

		changeItems: make([]abstract.ChangeItem, 0),
	}
}
