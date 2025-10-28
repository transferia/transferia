package queue

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/parsers/registry/blank"
	"go.ytsaurus.tech/library/go/core/log"
)

type MirrorSerializer struct {
	logger log.Logger
}

func (s *MirrorSerializer) serialize(changeItem *abstract.ChangeItem) ([]SerializedMessage, error) {
	if !changeItem.IsMirror() {
		return nil, xerrors.Errorf("MirrorSerializer should be used only with 'Mirror' changeItems")
	}
	key, err := changeitem.GetSequenceKey(changeItem)
	if err != nil {
		return nil, abstract.NewFatalError(xerrors.Errorf("unable to get sequence key: %w", err))
	}

	rawData, err := abstract.GetRawMessageData(*changeItem)
	if err != nil {
		return nil, abstract.NewFatalError(xerrors.Errorf("unable to get message: %w", err))
	}
	// no key, so round-robin partition assignment
	return []SerializedMessage{{Key: key, Value: rawData}}, nil
}

// Serialize
// naive implementation - can be boosted by multi-threading
func (s *MirrorSerializer) Serialize(input []abstract.ChangeItem) (map[abstract.TablePartID][]SerializedMessage, error) {
	if len(input) == 0 {
		return nil, nil
	}
	idToGroup := make(map[abstract.TablePartID][]SerializedMessage)
	for _, changeItem := range input {
		group, err := s.serialize(&changeItem)
		if err != nil {
			msg := "unable to serialize change item"
			logErrorWithChangeItem(s.logger, msg, err, &changeItem)
			return nil, xerrors.Errorf("%v: %w", msg, err)
		}
		if len(group) == 0 {
			continue
		}
		key, err := changeitem.GetSequenceKey(&changeItem)
		if err != nil {
			return nil, abstract.NewFatalError(xerrors.Errorf("unable to get sequence key: %w", err))
		}
		keyObject := abstract.TablePartID{
			TableID: abstract.TableID{
				Namespace: "",
				Name:      string(key),
			},
			PartID: "",
		}
		idToGroup[keyObject] = append(idToGroup[keyObject], group...)
	}
	return idToGroup, nil
}

func (s *MirrorSerializer) SerializeLB(changeItem *abstract.ChangeItem) ([]SerializedMessage, error) {
	isBlankChangedItem := func(item abstract.ChangeItem) bool {
		return len(item.ColumnNames) == len(blank.BlankCols) &&
			item.ColumnNames[blank.BlankColsIDX[blank.RawMessageColumn]] == blank.RawMessageColumn
	}
	if !isBlankChangedItem(*changeItem) {
		return nil, xerrors.Errorf("MirrorSerializer should be used only with BlankChangedItems")
	}
	rawData, ok := changeItem.ColumnValues[blank.BlankColsIDX[blank.RawMessageColumn]].([]byte)
	if !ok {
		return nil, xerrors.New("raw data must be presented for blank change item")
	}
	// no key, so round-robin partition assignment
	return []SerializedMessage{{Key: nil, Value: rawData}}, nil
}

// GroupAndSerializeLB
// For logbroker-destination logic should be absolute another!
// ChangeItems should be grouped by Key (it's ProducerID)
// And for every Key should be extracted extras (extras - unique for every producer)
func (s *MirrorSerializer) GroupAndSerializeLB(input []abstract.ChangeItem) (map[abstract.TablePartID][]SerializedMessage, map[abstract.TablePartID]map[string]string, error) {
	idToGroup := make(map[abstract.TablePartID][]SerializedMessage)
	extras := make(map[abstract.TablePartID]map[string]string)
	for _, changeItem := range input {
		group, err := s.SerializeLB(&changeItem)
		if err != nil {
			msg := "unable to serialize change item"
			logErrorWithChangeItem(s.logger, msg, err, &changeItem)
			return nil, nil, xerrors.Errorf("%v: %w", msg, err)
		}
		if len(group) == 0 {
			continue
		}
		sourceID, ok := changeItem.ColumnValues[blank.BlankColsIDX[blank.SourceIDColumn]].(string)
		if !ok {
			return nil, nil, xerrors.New("Source ID must be presented for blank delivery")
		}
		tableID := abstract.TablePartID{
			TableID: abstract.TableID{
				Namespace: "",
				Name:      sourceID,
			},
			PartID: "",
		}
		idToGroup[tableID] = append(idToGroup[tableID], group...)
		extras[tableID] = changeItem.ColumnValues[blank.BlankColsIDX[blank.ExtrasColumn]].(map[string]string)
	}
	return idToGroup, extras, nil
}

func NewMirrorSerializer(logger log.Logger) (*MirrorSerializer, error) {
	return &MirrorSerializer{
		logger: logger,
	}, nil
}
