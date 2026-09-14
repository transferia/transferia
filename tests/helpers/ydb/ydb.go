package ydb

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func TwoTablesEqual(t *testing.T, token, database, instance, tableA, tableB string) {
	tableAData := PullDataFromTable(t, token, database, instance, tableA)
	tableBData := PullDataFromTable(t, token, database, instance, tableB)
	require.Equal(t, len(tableAData), len(tableBData))
	sort.Slice(tableAData, func(i, j int) bool {
		return strings.Join(tableAData[i].KeyVals(), ".") < strings.Join(tableAData[j].KeyVals(), ".")
	})
	sort.Slice(tableBData, func(i, j int) bool {
		return strings.Join(tableBData[i].KeyVals(), ".") < strings.Join(tableBData[j].KeyVals(), ".")
	})
	for i := 0; i < len(tableAData); i++ {
		changeItemA, changeItemB := tableAData[i], tableBData[i]
		changeItemA.CommitTime = 0
		changeItemA.Table = "!"
		changeItemA.PartID = ""
		changeItemAStr := changeItemA.ToJSONString()
		changeItemB.CommitTime = 0
		changeItemB.Table = "!"
		changeItemB.PartID = ""
		changeItemBStr := changeItemB.ToJSONString()
		require.Equal(t, changeItemAStr, changeItemBStr)
	}
}

func PullDataFromTable(t *testing.T, token, database, instance, table string) []abstract.ChangeItem {
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(token),
		Database:           database,
		Instance:           instance,
		Tables:             []string{table},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		SecurityGroupIDs:   nil,
		Underlay:           false,
		ServiceAccountID:   "",
		UseFullPaths:       true,
		SAKeyContent:       "",
		ChangeFeedMode:     "",
		BufferSize:         0,
	}
	sinkMock := mocksink.NewMockSink(nil)
	targetMock := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkMock },
		Cleanup:       model.DisabledCleanup,
	}
	transferMock := transferhelpers.MakeTransfer("fake", src, &targetMock, abstract.TransferTypeSnapshotOnly)

	var extracted []abstract.ChangeItem

	sinkMock.PushCallback = func(input []abstract.ChangeItem) error {
		for _, currItem := range input {
			if currItem.Kind == abstract.InsertKind {
				require.NotZero(t, len(currItem.KeyCols()))
				extracted = append(extracted, currItem)
			}
		}
		return nil
	}
	helpers.Activate(t, transferMock)
	return extracted
}
