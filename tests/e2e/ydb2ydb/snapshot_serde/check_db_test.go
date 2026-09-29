package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	transformerhelpers "github.com/transferia/transferia/tests/helpers/transformer"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
)

var path = "dectest/timmyb32r-test"
var pathOut = "dectest/timmyb32r-test-out"
var sourceChangeItem abstract.ChangeItem

//---------------------------------------------------------------------------------------------------------------------

func serdeUdf(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
	for i := range items {
		items[i].Table = pathOut
		if items[i].Kind == abstract.InsertKind {
			sourceChangeItem = items[i]
			//--------------------------------------------------------------------------------
			changeItemStr := items[i].ToJSONString()
			unmarshalledChangeItem, err := abstract.UnmarshalChangeItem([]byte(changeItemStr))
			require.NoError(t, err)
			items[i] = *unmarshalledChangeItem
			//--------------------------------------------------------------------------------
			fmt.Printf("changeItem dump:%s\n", changeItemStr)
		}
	}
	return abstract.TransformerResult{
		Transformed: items,
		Errors:      nil,
	}
}

func anyTablesUdf(table abstract.TableID, schema abstract.TableColumns) bool {
	return true
}

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}

	t.Run("init source database", func(t *testing.T) {
		Target := &provider_ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		currChangeItem := testdata.YDBInitChangeItem(path)
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))
	})

	dst := &provider_ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	dst.WithDefaults()
	transfer := transferhelpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)

	serdeTransformer := transformerhelpers.NewSimpleTransformer(t, serdeUdf, anyTablesUdf)
	transformerhelpers.AddTransformer(t, transfer, serdeTransformer)

	t.Run("activate", func(t *testing.T) {
		delivery.Activate(t, transfer)
	})

	//-----------------------------------------------------------------------------------------------------------------
	// check

	sinkMock := mocksink.NewMockSink(nil)
	targetMock := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkMock },
		Cleanup:       model.DisabledCleanup,
	}
	transferMock := transferhelpers.MakeTransfer("fake", src, &targetMock, abstract.TransferTypeSnapshotOnly)
	var extractedChangeItem abstract.ChangeItem
	t.Run("extract change_item from dst", func(t *testing.T) {
		sinkMock.PushCallback = func(input []abstract.ChangeItem) error {
			for _, currItem := range input {
				if currItem.Table == pathOut && currItem.Kind == abstract.InsertKind {
					extractedChangeItem = currItem
				}
			}
			return nil
		}
		delivery.Activate(t, transferMock)
	})

	sourceChangeItem.CommitTime = 0
	sourceChangeItem.Table = "!"
	sourceChangeItem.PartID = ""
	sourceChangeItemStr := sourceChangeItem.ToJSONString()
	fmt.Printf("sourceChangeItemStr:%s\n", sourceChangeItemStr)

	extractedChangeItem.CommitTime = 0
	extractedChangeItem.Table = "!"
	extractedChangeItem.PartID = ""
	extractedChangeItemStr := extractedChangeItem.ToJSONString()
	fmt.Printf("extractedChangeItemStr:%s\n", extractedChangeItemStr)

	require.Equal(t, sourceChangeItemStr, extractedChangeItemStr)
}
