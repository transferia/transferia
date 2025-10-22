package queue

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/blank"
)

var mirrorSerializerTestMirrorChangeItem *abstract.ChangeItem

func init() {
	var testMirrorChangeItem = `{"id":0,"nextlsn":49,"commitTime":1648053051911000000,"txPosition":0,"kind":"insert","schema":"default-topic","table":"94","columnnames":["topic","partition","seq_no","write_time","data","meta"],"columnvalues":["default-topic",94,50,"2022-03-23T19:30:51.911+03:00","blablabla",null],"table_schema":[{"path":"","name":"topic","type":"utf8","key":true,"required":false,"original_type":"","original_type_params":null},{"path":"","name":"partition","type":"uint32","key":true,"required":false,"original_type":"","original_type_params":null},{"path":"","name":"seq_no","type":"uint64","key":true,"required":false,"original_type":"","original_type_params":null},{"path":"","name":"write_time","type":"datetime","key":true,"required":false,"original_type":"","original_type_params":null},{"path":"","name":"data","type":"utf8","key":false,"required":false,"original_type":"mirror:binary","original_type_params":null},{"path":"","name":"meta","type":"any","key":false,"required":false,"original_type":"","original_type_params":null}],"oldkeys":{},"tx_id":"","query":""}`
	mirrorSerializerTestMirrorChangeItem, _ = abstract.UnmarshalChangeItem([]byte(testMirrorChangeItem))
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
	require.Equal(t, len(batches[0].Key), 0)
	require.Equal(t, batches[0].Value, []byte(`blablabla`))
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
