package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
)

func TestGroup(t *testing.T) {
	//-----------------------------------------------------------------------------------------------------------------
	// prepare common part

	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		UseFullPaths:       false,
	}

	sinker := mocksink.NewMockSink(nil)
	dst := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}

	var changeItems []abstract.ChangeItem
	mutex := sync.Mutex{}
	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		mutex.Lock()
		defer mutex.Unlock()

		for _, currElem := range input {
			if currElem.Kind == abstract.InsertKind {
				changeItems = append(changeItems, currElem)
			}
		}
		return nil
	}

	//-----------------------------------------------------------------------------------------------------------------
	// init

	t.Run("init source database", func(t *testing.T) {
		Target := &provider_ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		require.NoError(t, sinker.Push([]abstract.ChangeItem{*testdata.YDBInitChangeItem("test_table/dir1/my_lovely_table")}))
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*testdata.YDBInitChangeItem("test_table/dir1/my_lovely_table2")}))

		require.NoError(t, sinker.Push([]abstract.ChangeItem{*testdata.YDBInitChangeItem("test_dir/dir1/table1")}))
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*testdata.YDBInitChangeItem("test_dir/dir2/table1")}))
	})

	//-----------------------------------------------------------------------------------------------------------------
	// check (UseFullPaths=false)

	runTestCase(t, "root", src, dst, &changeItems, false,
		nil,
		[]string{"test_table/dir1/my_lovely_table", "test_table/dir1/my_lovely_table2", "test_dir/dir1/table1", "test_dir/dir2/table1"},
	)
	runTestCase(t, "one table", src, dst, &changeItems, false,
		[]string{"test_table/dir1/my_lovely_table"},
		[]string{"my_lovely_table"},
	)
	runTestCase(t, "many tables", src, dst, &changeItems, false,
		[]string{"test_table/dir1/my_lovely_table", "test_table/dir1/my_lovely_table2"},
		[]string{"my_lovely_table", "my_lovely_table2"},
	)
	runTestCase(t, "directory 1", src, dst, &changeItems, false,
		[]string{"test_dir"},
		[]string{"test_dir/dir1/table1", "test_dir/dir2/table1"},
	)
	runTestCase(t, "directory 2", src, dst, &changeItems, false,
		[]string{"test_dir/dir1"},
		[]string{"dir1/table1"},
	)
	runTestCase(t, "directory 3", src, dst, &changeItems, false,
		[]string{"test_dir/dir1", "test_table/dir1/my_lovely_table"},
		[]string{"dir1/table1", "my_lovely_table"},
	)

	//-----------------------------------------------------------------------------------------------------------------
	// check (UseFullPaths=true)

	runTestCase(t, "root", src, dst, &changeItems, true,
		nil,
		[]string{"test_table/dir1/my_lovely_table", "test_table/dir1/my_lovely_table2", "test_dir/dir1/table1", "test_dir/dir2/table1"},
	)
	runTestCase(t, "one table", src, dst, &changeItems, true,
		[]string{"test_table/dir1/my_lovely_table"},
		[]string{"test_table/dir1/my_lovely_table"},
	)
	runTestCase(t, "many tables", src, dst, &changeItems, true,
		[]string{"test_table/dir1/my_lovely_table", "test_table/dir1/my_lovely_table2"},
		[]string{"test_table/dir1/my_lovely_table", "test_table/dir1/my_lovely_table2"},
	)
	runTestCase(t, "directory 1", src, dst, &changeItems, true,
		[]string{"test_dir"},
		[]string{"test_dir/dir1/table1", "test_dir/dir2/table1"},
	)
	runTestCase(t, "directory 2", src, dst, &changeItems, true,
		[]string{"test_dir/dir1"},
		[]string{"test_dir/dir1/table1"},
	)
	runTestCase(t, "directory 2", src, dst, &changeItems, true,
		[]string{"test_dir/dir1", "test_table/dir1/my_lovely_table"},
		[]string{"test_dir/dir1/table1", "test_table/dir1/my_lovely_table"},
	)
}

func runTestCase(t *testing.T, caseName string, src *provider_ydb.YdbSource, dst *model.MockDestination, changeItems *[]abstract.ChangeItem, useFullPath bool, pathsIn []string, pathsExpected []string) {
	fmt.Printf("starting test case: %s\n", caseName)
	src.UseFullPaths = useFullPath
	src.Tables = pathsIn
	*changeItems = make([]abstract.ChangeItem, 0)
	transfer := transferhelpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
	delivery.Activate(t, transfer)
	checkTableNameExpected(t, caseName, *changeItems, pathsExpected)
	fmt.Printf("finishing test case: %s\n", caseName)
}

func checkTableNameExpected(t *testing.T, caseName string, changeItems []abstract.ChangeItem, expectedBasePaths []string) {
	foundTableNames := make(map[string]bool)
	for _, currBasePath := range expectedBasePaths {
		foundTableNames[currBasePath] = false
	}

	expectedTableNamesStr, _ := json.Marshal(expectedBasePaths)
	fmt.Printf("checkTableNameExpected - expected table names:%s\n", expectedTableNamesStr)

	for _, currChangeItem := range changeItems {
		foundTableName := currChangeItem.Table
		fmt.Printf("checkTableNameExpected - found tableName:%s\n", foundTableName)
		_, ok := foundTableNames[foundTableName]
		require.True(t, ok)
		foundTableNames[foundTableName] = true
	}

	for _, v := range foundTableNames {
		require.True(t, v, fmt.Sprintf("failed %s case", caseName))
	}
}
