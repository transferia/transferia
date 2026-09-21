package incremental

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/pkg/worker/tasks"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydb_options "github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	ydb_table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestYDBIncrementalSnapshot(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		SecurityGroupIDs:   nil,
		Underlay:           false,
		ServiceAccountID:   "",
		UseFullPaths:       false,
		SAKeyContent:       "",
		ChangeFeedMode:     "",
		BufferSize:         0,
	}

	var readItems []abstract.ChangeItem
	var sinkLock sync.Mutex
	sinker := mocksink.NewMockSink(func(items []abstract.ChangeItem) error {
		items = yslices.Filter(items, func(i abstract.ChangeItem) bool {
			return i.IsRowEvent()
		})
		sinkLock.Lock()
		defer sinkLock.Unlock()
		readItems = append(readItems, items...)
		return nil
	})
	dst := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}

	db, err := ydb_go_sdk.Open(
		context.Background(),
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb_go_sdk.WithAccessTokenCredentials(
			os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		),
	)
	require.NoError(t, err)
	defer db.Close(context.Background())

	tables := []string{"test/table_c_int64", "test/table_c_string", "test/table_c_datetime"}
	initialValues := []string{"19", "'row  19'", strconv.Itoa(baseUnixTime + 19)}

	incremental := make([]abstract.IncrementalTable, 0, len(tables))
	for _, tablePath := range tables {
		keyCol := strings.TrimPrefix(tablePath, "test/table_")
		fullTablePath := fmt.Sprintf("%s/%s", src.Database, tablePath)
		require.NoError(t, createSampleTable(db, fullTablePath, keyCol))
		require.NoError(t, fillRowsRange(db, fullTablePath, 0, 50))
		// First check with zero initial state
		incremental = append(incremental, abstract.IncrementalTable{
			Name:         tablePath,
			Namespace:    "",
			CursorField:  keyCol,
			InitialState: "",
		})
	}

	transfer := transferhelpers.MakeTransfer("dttest", src, dst, abstract.TransferTypeSnapshotOnly)
	transfer.RegularSnapshot = &abstract.RegularSnapshot{Incremental: incremental}

	cpClient := coordinator.NewStatefulFakeClient()
	require.NoError(t, tasks.ActivateDelivery(context.Background(), nil, cpClient, *transfer, testmetrics.EmptyRegistry()))

	readTables := abstract.SplitByTableID(readItems)
	for _, tablePath := range tables {
		checkRows(t, readTables[*abstract.NewTableID("", tablePath)], 0, 50)
		fullTablePath := fmt.Sprintf("%s/%s", src.Database, tablePath)
		require.NoError(t, fillRowsRange(db, fullTablePath, 50, 100))
	}

	readItems = nil
	require.NoError(t, tasks.ActivateDelivery(context.Background(), nil, cpClient, *transfer, testmetrics.EmptyRegistry()))

	readTables = abstract.SplitByTableID(readItems)
	for _, tablePath := range tables {
		checkRows(t, readTables[*abstract.NewTableID("", tablePath)], 50, 100)
	}

	// Check non-empty initial state
	for i := range incremental {
		incremental[i].InitialState = initialValues[i]
	}
	// forgot current increment by using clean empty state
	cpClient = coordinator.NewStatefulFakeClient()
	readItems = nil
	require.NoError(t, tasks.ActivateDelivery(context.Background(), nil, cpClient, *transfer, testmetrics.EmptyRegistry()))

	readTables = abstract.SplitByTableID(readItems)
	for _, tablePath := range tables {
		checkRows(t, readTables[*abstract.NewTableID("", tablePath)], 20, 100)
	}
}

// checkRows checks whether rows contain unique rows numbered from expectedFrom to expectedTo
func checkRows(t *testing.T, rows []abstract.ChangeItem, expectedFrom, expectedTo int64) {
	require.Len(t, rows, int(expectedTo-expectedFrom))

	rowNumberSet := make(map[int64]struct{}, len(rows))
	max, min := int64(math.MinInt64), int64(math.MaxInt64)
	for _, row := range rows {
		rowNum := row.ColumnValues[row.ColumnNameIndex("c_int64")].(int64)
		rowNumberSet[rowNum] = struct{}{}
		if rowNum > max {
			max = rowNum
		}
		if rowNum < min {
			min = rowNum
		}
	}
	require.Equal(t, min, expectedFrom)
	require.Equal(t, max, expectedTo-1)
	require.Len(t, rowNumberSet, len(rows))
}

func createSampleTable(db *ydb_go_sdk.Driver, tablePath string, keyCol string) error {
	return db.Table().Do(context.Background(), func(ctx context.Context, s ydb_table.Session) error {
		return s.CreateTable(context.Background(), tablePath,
			ydb_options.WithColumn("c_int64", ydb_table_types.Optional(ydb_table_types.TypeInt64)),
			ydb_options.WithColumn("c_string", ydb_table_types.Optional(ydb_table_types.TypeString)),
			ydb_options.WithColumn("c_datetime", ydb_table_types.Optional(ydb_table_types.TypeDatetime)),
			ydb_options.WithPrimaryKeyColumn(keyCol),
		)
	})
}

func fillRowsRange(db *ydb_go_sdk.Driver, tablePath string, from, to int) error {
	return db.Table().Do(context.Background(), func(ctx context.Context, s ydb_table.Session) error {
		return s.BulkUpsert(context.Background(), tablePath, generateRows(from, to))
	})
}

const baseUnixTime = 1696183362

func generateRows(from, to int) ydb_table_types.Value {
	rows := make([]ydb_table_types.Value, 0, to-from)
	for i := from; i < to; i++ {
		rows = append(rows, ydb_table_types.StructValue(
			ydb_table_types.StructFieldValue("c_int64", ydb_table_types.Int64Value(int64(i))),
			ydb_table_types.StructFieldValue("c_string", ydb_table_types.BytesValue([]byte(fmt.Sprintf("row %3d", i)))),
			ydb_table_types.StructFieldValue("c_datetime", ydb_table_types.DatetimeValue(baseUnixTime+uint32(i))),
		))
	}
	return ydb_table_types.ListValue(rows...)
}
