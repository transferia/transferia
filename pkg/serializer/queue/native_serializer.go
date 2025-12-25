package queue

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

// NativeSerializer - for legacy compatibility: transfers named: realty-rent-prod/realty-rent-test
type NativeSerializer struct {
	batchingSettings model.Batching
	saveTxOrder      bool
}

func (s *NativeSerializer) serializeOneTableID(input []abstract.ChangeItem) []SerializedMessage {
	result := make([]SerializedMessage, 0)
	if s.batchingSettings.Enabled {
		result = append(result, BatchNative(s.batchingSettings, input)...)
	} else {
		for _, changeItem := range input {
			// Native parser can handle only array of ChangeItems, that's why we wrap changeItem to "[", "]".
			group := []SerializedMessage{{Key: []byte(changeItem.Fqtn()), Value: []byte("[" + changeItem.ToJSONString() + "]")}}
			result = append(result, group...)
		}
	}
	return result
}

// Serialize - serializes []abstract.ChangeItem into map: topic->[]SerializedMessage via json marshalling
// naive implementation - can be boosted by multi-threading
func (s *NativeSerializer) Serialize(input []abstract.ChangeItem) (map[abstract.TablePartID][]SerializedMessage, error) {
	if len(input) == 0 {
		return nil, nil
	}
	idToGroup := make(map[abstract.TablePartID][]SerializedMessage)
	if s.saveTxOrder {
		tablePartID := abstract.TablePartID{TableID: abstract.TableID{Namespace: "", Name: ""}, PartID: ""}
		idToGroup[tablePartID] = s.serializeOneTableID(input)
	} else {
		tableIDToChangeItems := splitByTablePartID(input)
		for tableID, changeItems := range tableIDToChangeItems {
			idToGroup[tableID] = s.serializeOneTableID(changeItems)
		}
	}
	return idToGroup, nil
}

func NewNativeSerializer(batchingSettings model.Batching, saveTxOrder bool) (*NativeSerializer, error) {
	return &NativeSerializer{
		batchingSettings: batchingSettings,
		saveTxOrder:      saveTxOrder,
	}, nil
}
