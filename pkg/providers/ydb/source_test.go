//go:build !disable_ydb_provider

package ydb

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const partitionsCount = 3

type asyncSinkMock struct {
	PushCallback func(items []abstract.ChangeItem)
}

func (s asyncSinkMock) AsyncPush(items []abstract.ChangeItem) chan error {
	errChan := make(chan error, 1)
	s.PushCallback(items)
	errChan <- nil
	return errChan
}

func (s asyncSinkMock) Close() error {
	return nil
}

func TestSourceCDC(t *testing.T) {
	db := ydbrecipe.Driver(t)
	transferID := "test_transfer"

	srcCfgTemplate := YdbSource{
		Instance: os.Getenv("YDB_ENDPOINT"),
		Database: os.Getenv("YDB_DATABASE"),
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
	}
	srcCfgTemplate.WithDefaults()

	t.Run("Simple", func(t *testing.T) {
		uniqKeysCount := 5
		tableName := "test_table"
		expectedItemsCount := prepareTableAndFeed(t, transferID, tableName, uniqKeysCount, 50)

		srcCfg := srcCfgTemplate
		srcCfg.Tables = []string{tableName}
		src, err := NewSource(transferID, &srcCfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		pushedItems := waitExpectedEvents(t, src, expectedItemsCount)

		checkEventsOrder(t, pushedItems, expectedItemsCount/uniqKeysCount)
	})

	t.Run("Many tables", func(t *testing.T) {
		uniqKeysCount := 5
		tableNames := []string{"test_many_table_1", "test_many_table_2", "test_many_table_3"}
		expectedItemsCount := 0
		expectedItemsCount += prepareTableAndFeed(t, transferID, tableNames[0], uniqKeysCount, 10)
		expectedItemsCount += prepareTableAndFeed(t, transferID, tableNames[1], uniqKeysCount, 10)
		expectedItemsCount += prepareTableAndFeed(t, transferID, tableNames[2], uniqKeysCount, 10)

		srcCfg := srcCfgTemplate
		srcCfg.Tables = tableNames
		src, err := NewSource(transferID, &srcCfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		pushedItems := waitExpectedEvents(t, src, expectedItemsCount)

		checkEventsOrder(t, pushedItems, expectedItemsCount/uniqKeysCount/len(tableNames))
	})

	t.Run("Custom feed", func(t *testing.T) {
		uniqKeysCount := 5
		tableName := "test_table_custom_feed"
		customFeedName := "custom_change_feed"
		expectedItemsCount := prepareTableAndFeed(t, customFeedName, tableName, uniqKeysCount, 20)

		srcCfg := srcCfgTemplate
		srcCfg.Tables = []string{tableName}
		srcCfg.ChangeFeedCustomName = customFeedName
		src, err := NewSource(transferID, &srcCfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		pushedItems := waitExpectedEvents(t, src, expectedItemsCount)

		checkEventsOrder(t, pushedItems, expectedItemsCount/uniqKeysCount)
	})

	t.Run("Compound primary key", func(t *testing.T) {
		uniqKeysCount := 5
		updatesPerKey := 10
		tableName := "test_table_compound_key"
		tablePath := formTablePath(tableName)
		createTableAndFeed(t, db, transferID, tablePath,
			options.WithColumn("id_int", types.Optional(types.TypeUint64)),
			options.WithColumn("id_string", types.Optional(types.TypeString)),
			options.WithColumn("val", types.Optional(types.TypeInt64)),
			options.WithPrimaryKeyColumn("id_int", "id_string"),
		)

		var upsertQueries []string
		for i := 0; i < uniqKeysCount; i++ {
			for j := 0; j < uniqKeysCount; j++ {
				for k := 1; k <= updatesPerKey; k++ {
					upsertQueries = append(upsertQueries,
						fmt.Sprintf("UPSERT INTO `%s` (id_int, id_string, val) VALUES (%d, '%d', %d);", tablePath, i, j, k),
					)
				}
			}
		}
		execQueries(t, db, upsertQueries)

		srcCfg := srcCfgTemplate
		srcCfg.Tables = []string{tableName}
		src, err := NewSource(transferID, &srcCfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		pushedItems := waitExpectedEvents(t, src, len(upsertQueries))

		checkEventsOrder(t, pushedItems, len(upsertQueries)/uniqKeysCount/uniqKeysCount)
	})

	t.Run("Sending synchronize events", func(t *testing.T) {
		t.Skip()
		// TODO - implement after TM-7382
	})

	t.Run("Use full path", func(t *testing.T) {
		t.Skip()
		// TODO - now it doesn't work
	})

	t.Run("Get up to date table schema", func(t *testing.T) {
		checkSchemaUpdateWithMode(t, db, transferID, "UPDATES", srcCfgTemplate)
		checkSchemaUpdateWithMode(t, db, transferID, "NEW_IMAGE", srcCfgTemplate)
		checkSchemaUpdateWithMode(t, db, transferID, "NEW_AND_OLD_IMAGES", srcCfgTemplate)
	})

	t.Run("Canon", func(t *testing.T) {
		tableName := "test_table_canon"
		tablePath := formTablePath(tableName)
		createTableAndFeed(t, db, transferID, tablePath,
			options.WithColumn("id_int", types.Optional(types.TypeUint64)),
			options.WithColumn("id_string", types.Optional(types.TypeString)),
			options.WithColumn("val_int", types.Optional(types.TypeInt64)),
			options.WithColumn("val_datetime", types.Optional(types.TypeDatetime)),
			options.WithPrimaryKeyColumn("id_int", "id_string"),
		)

		upsertQuery := func(idInt int, idStr string, valInt int, valDatetime string) string {
			return fmt.Sprintf("UPSERT INTO `%s` (id_int, id_string, val_int, val_datetime) VALUES (%d, '%s', %d, Datetime('%s'));",
				tablePath, idInt, idStr, valInt, valDatetime)
		}
		deleteQuery := func(idInt int, idStr string) string {
			return fmt.Sprintf("DELETE FROM `%s` WHERE id_int = %d AND id_string = '%s';", tablePath, idInt, idStr)
		}

		upsertQueries := []string{
			upsertQuery(1, "key_1", 123, "2019-09-16T00:00:00Z"),
			deleteQuery(1, "key_1"),
		}
		execQueries(t, db, upsertQueries)

		srcCfg := srcCfgTemplate
		srcCfg.Tables = []string{tableName}
		src, err := NewSource(transferID, &srcCfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		pushedItems := waitExpectedEvents(t, src, len(upsertQueries))
		for i := range pushedItems {
			pushedItems[i].CommitTime = 0
			pushedItems[i].LSN = 0
		}

		canon.SaveJSON(t, pushedItems)
	})
}

func checkSchemaUpdateWithMode(t *testing.T, db *ydb.Driver, transferID string, mode ChangeFeedModeType, srcCfgTemplate YdbSource) {
	tableName := "schema_up_to_date_new_image" + "_" + string(mode)
	tablePath := formTablePath(tableName)
	createTableAndFeedWithMode(t, db, transferID, tablePath, mode,
		options.WithColumn("id", types.Optional(types.TypeUint64)),
		options.WithColumn("val", types.Optional(types.TypeString)),
		options.WithPrimaryKeyColumn("id"),
	)

	execQueries(t, db, []string{
		fmt.Sprintf("UPSERT INTO `%s` (id, val) VALUES (%d, '%s');", tablePath, 1, "val_1"),
	})

	srcCfg := srcCfgTemplate
	srcCfg.Tables = []string{tableName}
	src, err := NewSource(transferID, &srcCfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	pushedItems := make([]abstract.ChangeItem, 0)
	sink := &asyncSinkMock{
		PushCallback: func(items []abstract.ChangeItem) {
			pushedItems = append(pushedItems, items...)
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	errChan := make(chan error, 1)
	go func() {
		errChan <- src.Run(sink)
		wg.Done()
	}()

	waitingStartTime := time.Now()
	for len(pushedItems) != 1 {
		require.False(t, time.Since(waitingStartTime) > time.Second*20)
	}

	require.NoError(t, db.Table().Do(context.Background(), func(ctx context.Context, s table.Session) error {
		return s.AlterTable(ctx, tablePath, options.WithAddColumn("new_val", types.Optional(types.TypeString)))
	}))
	execQueries(t, db, []string{
		fmt.Sprintf("UPSERT INTO `%s` (id, val, new_val) VALUES (%d, '%s', '%s');", tablePath, 2, "val_2", "new_val_2"),
	})

	for len(pushedItems) != 2 {
		require.False(t, time.Since(waitingStartTime) > time.Second*20)
	}

	src.Stop()
	wg.Wait()
	if err := <-errChan; err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}

	require.Len(t, pushedItems, 2)
	require.Equal(t, []string{"id", "val"}, pushedItems[0].ColumnNames)
	require.Equal(t, []string{"id", "new_val", "val"}, pushedItems[1].ColumnNames)
}

// events with the same primary keys are ordered,
// but not ordered relative to events for records with other keys
func checkEventsOrder(t *testing.T, events []abstract.ChangeItem, expectedVal int) {
	if len(events) == 0 {
		return
	}

	keysCount := 0
	for _, col := range events[0].TableSchema.Columns() {
		if col.IsKey() {
			keysCount++
		} else {
			break
		}
	}
	require.Equal(t, len(events[0].ColumnNames), keysCount+1, "For test should be one not key column at the end")

	keyEventVal := make(map[string]int64)
	for _, event := range events {
		key := strings.Join(append(event.KeyVals(), event.Table), "|||")
		val := event.ColumnValues[keysCount].(int64)

		if _, ok := keyEventVal[key]; ok {
			require.True(t, val > keyEventVal[key])
		}
		keyEventVal[key] = val
	}

	for _, val := range keyEventVal {
		require.Equal(t, int64(expectedVal), val)
	}
}

func waitExpectedEvents(t *testing.T, src *Source, expectedItemsCount int) []abstract.ChangeItem {
	pushedItems := make([]abstract.ChangeItem, 0, expectedItemsCount)

	sink := &asyncSinkMock{
		PushCallback: func(items []abstract.ChangeItem) {
			pushedItems = append(pushedItems, items...)
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	errChan := make(chan error, 1)
	go func() {
		errChan <- src.Run(sink)
		wg.Done()
	}()

	for len(pushedItems) != expectedItemsCount {
	}
	src.Stop()
	wg.Wait()
	if err := <-errChan; err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}

	return pushedItems
}

func prepareTableAndFeed(t *testing.T, feedName, tableName string, differentKeysCount, updatesPerKey int) int {
	db := ydbrecipe.Driver(t)
	tablePath := formTablePath(tableName)
	createTableAndFeed(t, db, feedName, tablePath,
		options.WithColumn("id", types.Optional(types.TypeUint64)),
		options.WithColumn("val", types.Optional(types.TypeInt64)),
		options.WithPrimaryKeyColumn("id"),
	)

	var upsertQueries []string
	for i := 0; i < differentKeysCount; i++ {
		for j := 1; j <= updatesPerKey; j++ {
			upsertQueries = append(upsertQueries,
				fmt.Sprintf("UPSERT INTO `%s` (id, val) VALUES (%d, %d);", tablePath, uint64(i), int64(j)),
			)
		}
	}
	execQueries(t, db, upsertQueries)

	return len(upsertQueries)
}

func execQueries(t *testing.T, db *ydb.Driver, queries []string) {
	require.NoError(t, db.Table().Do(context.Background(), func(ctx context.Context, s table.Session) error {
		writeTx := table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		)
		for _, q := range queries {
			if _, _, err := s.Execute(ctx, writeTx, q, nil); err != nil {
				return err
			}
		}
		return nil
	}))
}

func createTableAndFeed(t *testing.T, db *ydb.Driver, feedName, tablePath string, opts ...options.CreateTableOption) {
	createTableAndFeedWithMode(t, db, feedName, tablePath, ChangeFeedModeNewImage, opts...)
}

func createTableAndFeedWithMode(t *testing.T, db *ydb.Driver, feedName, tablePath string, mode ChangeFeedModeType, opts ...options.CreateTableOption) {
	opts = append(opts, options.WithPartitions(options.WithUniformPartitions(partitionsCount)))

	require.NoError(t, db.Table().Do(context.Background(), func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, tablePath, opts...)
	}))

	require.NoError(t, createChangeFeedOneTable(context.Background(), db, tablePath, feedName, &YdbSource{ChangeFeedMode: mode}))
}

func formTablePath(tableName string) string {
	return "/" + path.Join(os.Getenv("YDB_DATABASE"), tableName)
}
