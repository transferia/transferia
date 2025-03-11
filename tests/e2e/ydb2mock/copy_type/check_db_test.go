package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/ydb"
	"github.com/transferria/transferria/tests/helpers"
)

func TestGroup(t *testing.T) {
	//-----------------------------------------------------------------------------------------------------------------
	// prepare common part

	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		UseFullPaths:       false,
	}

	sinker := &helpers.MockSink{}
	dst := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}

	var changeItems []abstract.ChangeItem
	mutex := sync.Mutex{}
	sinker.PushCallback = func(input []abstract.ChangeItem) {
		mutex.Lock()
		defer mutex.Unlock()

		for _, currElem := range input {
			if currElem.Kind == abstract.InsertKind {
				changeItems = append(changeItems, currElem)
			}
		}
	}

	//-----------------------------------------------------------------------------------------------------------------
	// init

	t.Run("init source database", func(t *testing.T) {
		Target := &ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		require.NoError(t, sinker.Push([]abstract.ChangeItem{*helpers.YDBInitChangeItem("test_table/dir1/my_lovely_table")}))
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*helpers.YDBInitChangeItem("test_table/dir1/my_lovely_table2")}))

		require.NoError(t, sinker.Push([]abstract.ChangeItem{*helpers.YDBInitChangeItem("test_dir/dir1/table1")}))
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*helpers.YDBInitChangeItem("test_dir/dir2/table1")}))
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

func runTestCase(t *testing.T, caseName string, src *ydb.YdbSource, dst *model.MockDestination, changeItems *[]abstract.ChangeItem, useFullPath bool, pathsIn []string, pathsExpected []string) {
	fmt.Printf("starting test case: %s\n", caseName)
	src.UseFullPaths = useFullPath
	src.Tables = pathsIn
	*changeItems = make([]abstract.ChangeItem, 0)
	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
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
