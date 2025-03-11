package queue

import (
	"runtime"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/debezium"
	"github.com/transferria/transferria/pkg/debezium/packer"
	lightningcache "github.com/transferria/transferria/pkg/debezium/packer/lightning_cache"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
	"go.ytsaurus.tech/library/go/core/log"
)

const defaultTopicPrefix = "__data_transfer_stub"

type DebeziumSerializer struct {
	parameters  map[string]string
	isSnapshot  bool
	emitter     *debezium.Emitter
	saveTxOrder bool
	logger      log.Logger
}

func (s *DebeziumSerializer) serialize(changeItem *abstract.ChangeItem, packerCache packer.SessionPackers) ([]SerializedMessage, error) {
	arr, err := s.emitter.EmitKV(changeItem, debezium.GetPayloadTSMS(changeItem), s.isSnapshot, packerCache)
	if err != nil {
		return nil, xerrors.Errorf("failed to emit debezium messages: %w", err)
	}
	result := make([]SerializedMessage, 0, len(arr))
	for _, el := range arr {
		var msg SerializedMessage
		if el.DebeziumVal == nil {
			msg = SerializedMessage{Key: []byte(el.DebeziumKey), Value: nil}
		} else {
			msg = SerializedMessage{Key: []byte(el.DebeziumKey), Value: []byte(*el.DebeziumVal)}
		}
		result = append(result, msg)
	}
	return result, nil
}

func (s *DebeziumSerializer) SerializeOneThread(input []abstract.ChangeItem, packerCache packer.SessionPackers) (map[abstract.TablePartID][]SerializedMessage, error) {
	if len(input) == 0 {
		return nil, nil
	}
	idToGroup := make(map[abstract.TablePartID][]SerializedMessage)
	for _, changeItem := range input {
		group, err := s.serialize(&changeItem, packerCache)
		if err != nil {
			msg := "unable to serialize change item"
			logErrorWithChangeItem(s.logger, msg, err, &changeItem)
			return nil, xerrors.Errorf("%v: %w", msg, err)
		}
		if len(group) == 0 {
			continue
		}
		tableID := abstract.TablePartID{TableID: abstract.TableID{Namespace: "", Name: ""}, PartID: ""}
		if !s.saveTxOrder {
			tableID = changeItem.TablePartID()
		}
		idToGroup[tableID] = append(idToGroup[tableID], group...)
	}
	return idToGroup, nil
}

func (s *DebeziumSerializer) SerializeImpl(input []abstract.ChangeItem, threadsNum, chunkSize int) (map[abstract.TablePartID][]SerializedMessage, error) {
	if len(input) == 0 {
		return nil, nil
	}

	packerCache, err := lightningcache.NewLightningCache(s.emitter, input, s.isSnapshot)
	if err != nil {
		return nil, xerrors.Errorf("unable to build packerCache, err: %w", err)
	}

	result, err := MultithreadingSerialize(s, packerCache, input, threadsNum, chunkSize, s.emitter.MaxMessageSize())
	if err != nil {
		return nil, xerrors.Errorf("unable to serialize, err: %w", err)
	}

	return result, nil
}

// Serialize - serializes []abstract.ChangeItem into map: topic->[]SerializedMessage via debezium emitter
// It's optimized version - with multithreading and caches optimizations
func (s *DebeziumSerializer) Serialize(input []abstract.ChangeItem) (map[abstract.TablePartID][]SerializedMessage, error) {
	// runtime.NumCPU()*4 showed the best performance in benchmark (pkg/debezium/bench)
	// chunkSize==16 showed one of the best performance (with dropKeys:false)
	// chunkSize==64 showed one of the best performance (with dropKeys:true)
	return s.SerializeImpl(input, runtime.NumCPU()*4, 64)
}

func MakeFormatSettingsWithTopicPrefix(format model.SerializationFormat, topicPrefix string, topicFullPath string) model.SerializationFormat {
	formatSettingsCopy := make(map[string]string) // to not modify 'formatSettings'
	for k, v := range format.Settings {
		formatSettingsCopy[k] = v
	}
	if _, ok := formatSettingsCopy[debeziumparameters.TopicPrefix]; !ok {
		if topicPrefix != "" {
			formatSettingsCopy[debeziumparameters.TopicPrefix] = topicPrefix
		} else if topicFullPath != "" {
			formatSettingsCopy[debeziumparameters.TopicPrefix] = topicFullPath
		} else {
			formatSettingsCopy[debeziumparameters.TopicPrefix] = defaultTopicPrefix
		}
	}
	return model.SerializationFormat{
		Name:             format.Name,
		Settings:         formatSettingsCopy,
		SettingsKV:       format.SettingsKV,
		BatchingSettings: format.BatchingSettings,
	}
}

func NewDebeziumSerializer(formatSettings map[string]string, saveTxOrder, dropKeys, isSnapshot bool, logger log.Logger) (*DebeziumSerializer, error) {
	emitter, err := debezium.NewMessagesEmitter(formatSettings, "1.1.2.Final", dropKeys, logger) // TODO - finalize database & parameters & version
	if err != nil {
		return nil, xerrors.Errorf("can't create debezium emitter: %w", err)
	}
	return &DebeziumSerializer{
		parameters:  formatSettings,
		isSnapshot:  isSnapshot,
		emitter:     emitter,
		saveTxOrder: saveTxOrder,
		logger:      logger,
	}, nil
}
