package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
)

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}

	t.Run("init source database", func(t *testing.T) {
		Target := &ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		require.NoError(t, sinker.Push([]abstract.ChangeItem{*helpers.YDBInitChangeItem("in/test_table/dir1/my_lovely_table")}))
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*helpers.YDBInitChangeItem("in/test_table/dir1/my_lovely_table2")}))

		require.NoError(t, sinker.Push([]abstract.ChangeItem{*helpers.YDBInitChangeItem("in/test_dir/dir1/table1")}))
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*helpers.YDBInitChangeItem("in/test_dir/dir2/table1")}))
	})

	dst := &ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Cleanup:  "Disabled",
	}
	dst.WithDefaults()

	//-----------------------------------------------------------------------------------------------------------------
	// check (UseFullPaths=false)

	runTestCase(t, "root", src, dst, false,
		nil,
		[]string{"out_root/in/test_table/dir1/my_lovely_table", "out_root/in/test_table/dir1/my_lovely_table2", "out_root/in/test_dir/dir1/table1", "out_root/in/test_dir/dir2/table1"},
	)
	runTestCase(t, "one_table", src, dst, false,
		[]string{"in/test_table/dir1/my_lovely_table"},
		[]string{"out_one_table/my_lovely_table"},
	)
	runTestCase(t, "many_tables", src, dst, false,
		[]string{"in/test_table/dir1/my_lovely_table", "in/test_table/dir1/my_lovely_table2"},
		[]string{"out_many_tables/my_lovely_table", "out_many_tables/my_lovely_table2"},
	)
	runTestCase(t, "directory_case1", src, dst, false,
		[]string{"in/test_dir"},
		[]string{"out_directory_case1/test_dir/dir1/table1", "out_directory_case1/test_dir/dir2/table1"},
	)
	runTestCase(t, "directory_case2", src, dst, false,
		[]string{"in/test_dir/dir1"},
		[]string{"out_directory_case2/dir1/table1"},
	)
	runTestCase(t, "table_and_directory", src, dst, false,
		[]string{"in/test_dir/dir1", "in/test_table/dir1/my_lovely_table"},
		[]string{"out_table_and_directory/dir1/table1", "out_table_and_directory/my_lovely_table"},
	)

	//-----------------------------------------------------------------------------------------------------------------
	// check (UseFullPaths=true)

	runTestCase(t, "root_FULL_PATHS", src, dst, true,
		nil,
		[]string{"out_root_FULL_PATHS/in/test_table/dir1/my_lovely_table", "out_root_FULL_PATHS/in/test_table/dir1/my_lovely_table2", "out_root_FULL_PATHS/in/test_dir/dir1/table1", "out_root_FULL_PATHS/in/test_dir/dir2/table1"},
	)
	runTestCase(t, "one_table_FULL_PATHS", src, dst, true,
		[]string{"in/test_table/dir1/my_lovely_table"},
		[]string{"out_one_table_FULL_PATHS/in/test_table/dir1/my_lovely_table"},
	)
	runTestCase(t, "many_tables_FULL_PATHS", src, dst, true,
		[]string{"in/test_table/dir1/my_lovely_table", "in/test_table/dir1/my_lovely_table2"},
		[]string{"out_many_tables_FULL_PATHS/in/test_table/dir1/my_lovely_table", "out_many_tables_FULL_PATHS/in/test_table/dir1/my_lovely_table2"},
	)
	runTestCase(t, "directory_case1_FULL_PATHS", src, dst, true,
		[]string{"in/test_dir"},
		[]string{"out_directory_case1_FULL_PATHS/in/test_dir/dir1/table1", "out_directory_case1_FULL_PATHS/in/test_dir/dir2/table1"},
	)
	runTestCase(t, "directory_case2_FULL_PATHS", src, dst, true,
		[]string{"in/test_dir/dir1"},
		[]string{"out_directory_case2_FULL_PATHS/in/test_dir/dir1/table1"},
	)
	runTestCase(t, "table_and_directory_FULL_PATHS", src, dst, true,
		[]string{"in/test_dir/dir1", "in/test_table/dir1/my_lovely_table"},
		[]string{"out_table_and_directory_FULL_PATHS/in/test_dir/dir1/table1", "out_table_and_directory_FULL_PATHS/in/test_table/dir1/my_lovely_table"},
	)
}

func runTestCase(t *testing.T, caseName string, src *ydb.YdbSource, dst *ydb.YdbDestination, useFullPath bool, pathsIn []string, pathsExpected []string) {
	fmt.Printf("starting test case: %s\n", caseName)
	src.UseFullPaths = useFullPath
	src.Tables = pathsIn
	dst.Path = fmt.Sprintf("out_%s", caseName)
	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	checkTables(t, caseName, src, pathsExpected)
	fmt.Printf("finishing test case: %s\n", caseName)
}

func checkTables(t *testing.T, caseName string, src *ydb.YdbSource, expectedPaths []string) {
	src.Tables = nil
	storage, err := ydb.NewStorage(src.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	tableMap, err := storage.TableList(nil)
	require.NoError(t, err)

	expectedTableNamesStr, _ := json.Marshal(expectedPaths)
	fmt.Printf("checkTables - expected table names:%s\n", expectedTableNamesStr)

	expectedPathsMap := make(map[string]bool)
	for _, currPath := range expectedPaths {
		expectedPathsMap[currPath] = false
	}
	for table := range tableMap {
		fmt.Printf("checkTables - found path:%s\n", table.Name)
		if _, ok := expectedPathsMap[table.Name]; ok {
			expectedPathsMap[table.Name] = true
		}
	}

	for _, v := range expectedPathsMap {
		require.True(t, v, fmt.Sprintf("failed %s case", caseName))
	}
}
