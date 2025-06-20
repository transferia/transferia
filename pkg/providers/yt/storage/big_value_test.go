//go:build !disable_yt_provider

package storage

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/cleanup"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"github.com/transferia/transferia/pkg/providers/yt/sink"
)

type TestObject struct {
	Data string `yson:"data"`
}

func TestBigValue(t *testing.T) {
	_, cancel := recipe.NewEnv(t)
	defer cancel()

	maxRetriesCount := sink.MaxRetriesCount
	sink.MaxRetriesCount = 1
	defer func() {
		sink.MaxRetriesCount = maxRetriesCount
	}()

	dstModel := yt.NewYtDestinationV1(yt.YtDestination{
		Path:          "//home/cdc/test/big_value",
		CellBundle:    "default",
		PrimaryMedium: "default",
		Cluster:       os.Getenv("YT_PROXY"),
	})
	dstModel.WithDefaults()

	changeItems := []abstract.ChangeItem{
		{
			Kind:  abstract.InsertKind,
			Table: "test_table",
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName: "key",
					DataType:   "utf8",
					PrimaryKey: true,
				},
				{
					ColumnName: "value",
					DataType:   "any",
					PrimaryKey: false,
				},
			}),
			ColumnNames: []string{
				"key",
				"value",
			},
			ColumnValues: []interface{}{
				"1",
				&TestObject{
					Data: strings.Repeat("1", 16*1024*1024+1),
				},
			},
		},
	}

	t.Run("do_not_discard_big_values", func(t *testing.T) {
		sinker, err := sink.NewSinker(dstModel, "big_value", logger.Log, emptyRegistry(), coordinator.NewFakeClient(), nil)
		require.NoError(t, err)
		defer cleanup.Close(sinker, logger.Log)

		err = sinker.Push(changeItems)
		require.Error(t, err)
	})

	t.Run("discard_big_values", func(t *testing.T) {
		dstModel.LegacyModel().(*yt.YtDestination).DiscardBigValues = true

		sinker, err := sink.NewSinker(dstModel, "big_value", logger.Log, emptyRegistry(), coordinator.NewFakeClient(), nil)
		require.NoError(t, err)
		defer cleanup.Close(sinker, logger.Log)

		err = sinker.Push(changeItems)
		require.NoError(t, err)
	})
}
