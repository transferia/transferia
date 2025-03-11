package queue

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/serializer"
	"go.ytsaurus.tech/library/go/core/log"
)

type JSONSerializer struct {
	batchingSettings model.Batching
	saveTxOrder      bool
	logger           log.Logger
}

var UnsupportedItemKinds = map[abstract.Kind]bool{
	abstract.UpdateKind: true,
	abstract.DeleteKind: true,
}

func serializeQueueItemToJSON(changeItem *abstract.ChangeItem) ([]byte, error) {
	return serializer.NewJSONSerializer(
		&serializer.JSONSerializerConfig{
			UnsupportedItemKinds: UnsupportedItemKinds,
			AddClosingNewLine:    false,
			AnyAsString:          false,
		}).Serialize(changeItem)
}

func (s *JSONSerializer) serialize(changeItem *abstract.ChangeItem) ([]SerializedMessage, error) {
	buf, err := serializeQueueItemToJSON(changeItem)
	if err != nil {
		return nil, xerrors.Errorf("unable to serialize JSON: %w", err)
	}
	return []SerializedMessage{{Key: []byte(changeItem.Fqtn()), Value: buf}}, nil
}

func (s *JSONSerializer) serializeOneTableID(input []abstract.ChangeItem) ([]SerializedMessage, error) {
	if s.batchingSettings.Enabled {
		batch, err := BatchJSON(s.batchingSettings, input)
		if err != nil {
			return nil, xerrors.Errorf("unable to batch json, err: %w", err)
		}
		return batch, nil
	} else {
		result := make([]SerializedMessage, 0, len(input))
		for _, changeItem := range input {
			batch, err := s.serialize(&changeItem)
			if err != nil {
				return nil, xerrors.Errorf("unable to serialize json, err: %w", err)
			}
			result = append(result, batch...)
		}
		return result, nil
	}
}

func (s *JSONSerializer) Serialize(input []abstract.ChangeItem) (map[abstract.TablePartID][]SerializedMessage, error) {
	if len(input) == 0 {
		return nil, nil
	}
	var idToGroup map[abstract.TablePartID][]SerializedMessage
	if s.saveTxOrder {
		tablePartID := abstract.TablePartID{TableID: abstract.TableID{Namespace: "", Name: ""}, PartID: ""}
		serializedMessages, err := s.serializeOneTableID(input)
		if err != nil {
			return nil, xerrors.Errorf("unable to serialize input, err: %w", err)
		}
		idToGroup = map[abstract.TablePartID][]SerializedMessage{tablePartID: serializedMessages}
	} else {
		tableIDToChangeItems := splitByTablePartID(input)
		idToGroup = make(map[abstract.TablePartID][]SerializedMessage, len(tableIDToChangeItems))
		for tableID, changeItems := range tableIDToChangeItems {
			serializedMessages, err := s.serializeOneTableID(changeItems)
			if err != nil {
				return nil, xerrors.Errorf("unable to serialize messages for table, err: %w", err)
			}
			idToGroup[tableID] = serializedMessages
		}
	}
	return idToGroup, nil
}

func NewJSONSerializer(batchingSettings model.Batching, saveTxOrder bool, logger log.Logger) (*JSONSerializer, error) {
	return &JSONSerializer{
		batchingSettings: batchingSettings,
		saveTxOrder:      saveTxOrder,
		logger:           logger,
	}, nil
}
