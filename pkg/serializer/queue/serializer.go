package queue

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium/packer"
)

type SerializedMessage struct {
	Key   []byte
	Value []byte
}

// Serializer - takes array of changeItems, returns queue messages, grouped by some groupID (string)
// all messages of one group should go in the same partition (that's why TopicName field in SerializedMessagesGroup struct)
//
// this separation: groupID vs topicPath - is useful mostly for logbroker-sink (where groupID is part of sourceID)
//
//	mirror-serializer should keep order into sourceID
//	    so, groupID for mirror is sourceID
//	other serializers should be able to write different tables into different partitions simultaneously
//	    (for sharding runtime case - when a lot of workers write into lb simultaneously)
//	    so, groupID for other serializers is abstract.TablePartID
type Serializer interface {
	Serialize(input []abstract.ChangeItem) (map[abstract.TablePartID][]SerializedMessage, error)
}

type serializerOneThread interface {
	SerializeOneThread(input []abstract.ChangeItem, packerCache packer.SessionPackers) (map[abstract.TablePartID][]SerializedMessage, error)
}
