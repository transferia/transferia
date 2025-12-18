package main

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

const testTableName = "test_table/my_lovely_table"

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

	t.Run("init source database", func(t *testing.T) {
		Target := &ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		require.NoError(t, sinker.Push([]abstract.ChangeItem{*helpers.YDBInitChangeItem(testTableName)}))
	})

	runTestCase(t, "no filter", src, dst, &changeItems,
		[]string{},
		[]string{},
		true,
	)
	runTestCase(t, "filter on source", src, dst, &changeItems,
		[]string{testTableName},
		[]string{},
		false,
	)
	runTestCase(t, "filter on transfer", src, dst, &changeItems,
		[]string{},
		[]string{testTableName},
		false,
	)
}

func runTestCase(t *testing.T, caseName string, src *ydb.YdbSource, dst *model.MockDestination, changeItems *[]abstract.ChangeItem, srcTables []string, includeObjects []string, isError bool) {
	fmt.Printf("starting test case: %s\n", caseName)
	src.Tables = srcTables
	*changeItems = make([]abstract.ChangeItem, 0)

	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: includeObjects}
	_, err := helpers.ActivateErr(transfer)
	if isError {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
		require.Equal(t, len(*changeItems), 1)
	}
	fmt.Printf("finishing test case: %s\n", caseName)
}
