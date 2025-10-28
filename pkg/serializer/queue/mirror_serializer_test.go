package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/blank"
)

var mirrorSerializerTestMirrorChangeItem *abstract.ChangeItem

func init() {
	tmp := abstract.MakeRawMessage([]byte("stub"), "", time.Now(), "", 0, 0, []byte("aboba123"))
	mirrorSerializerTestMirrorChangeItem = &tmp
}

func TestMirrorSerializerEmptyInput(t *testing.T) {
	mirrorSerializer, err := NewMirrorSerializer(nil)
	require.NoError(t, err)

	batches, err := mirrorSerializer.Serialize([]abstract.ChangeItem{})
	require.NoError(t, err)
	require.Len(t, batches, 0)
}

func TestMirrorSerializerTopicName(t *testing.T) {
	mirrorSerializer, err := NewMirrorSerializer(logger.Log)
	require.NoError(t, err)

	batches, err := mirrorSerializer.serialize(mirrorSerializerTestMirrorChangeItem)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Equal(t, batches[0].Key, []byte("stub"))
	require.Equal(t, batches[0].Value, []byte(`aboba123`))
}

func TestSerializeLB(t *testing.T) {
	changeItemsCount := 10
	changeItems := make([]abstract.ChangeItem, 0)
	for i := 0; i < changeItemsCount; i++ {
		changeItems = append(changeItems, blank.NewRawMessage(parsers.Message{}, abstract.Partition{Cluster: "", Partition: 0, Topic: ""}))
		sourceIDIndex := blank.BlankColsIDX[blank.SourceIDColumn]
		changeItems[len(changeItems)-1].ColumnValues[sourceIDIndex] = fmt.Sprintf("%d", i)
	}
	mirrorSerializer, err := NewMirrorSerializer(logger.Log)
	require.NoError(t, err)
	batches, extras, err := mirrorSerializer.GroupAndSerializeLB(changeItems)
	require.NoError(t, err)
	require.Equal(t, changeItemsCount, len(extras))
	require.Equal(t, len(batches), len(extras))
}
