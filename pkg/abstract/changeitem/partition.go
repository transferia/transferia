package changeitem

import (
	"fmt"
	"strings"
)

type Partition struct {
	Cluster   string `json:"cluster"`
	Partition uint32 `json:"partition"`
	Topic     string `json:"topic"`
}

func (p Partition) String() string {
	return fmt.Sprintf("{\"cluster\":\"%s\",\"partition\":%d,\"topic\":\"%s\"}", p.Cluster, p.Partition, p.Topic)
}

func (p Partition) LegacyShittyString() string {
	slashes := strings.Count(p.Topic, "/")
	oldFashionTopic := strings.ReplaceAll(strings.Replace(p.Topic, "/", "@", slashes-1), "/", "--")
	return fmt.Sprintf("rt3.%s--%s:%v", p.Cluster, oldFashionTopic, p.Partition)
}

func NewPartition(topic string, partition uint32) Partition {
	if !strings.HasPrefix(topic, "rt3.") {
		return Partition{
			Partition: partition,
			Cluster:   "",
			Topic:     topic,
		}
	}

	// for pqv0 legacy topic with cluster string
	// will be simplified in TM-9591
	if len(topic) < 4 {
		return Partition{
			Partition: partition,
			Cluster:   "",
			Topic:     "",
		}
	}
	topic = topic[4:]

	splittedParts := strings.Split(topic, "--")
	if len(splittedParts) == 1 {
		return Partition{
			Partition: partition,
			Cluster:   "",
			Topic:     "",
		}
	}

	topicName := strings.ReplaceAll(topic[len(splittedParts[0])+2:], "--", "/")
	topicName = strings.ReplaceAll(topicName, "@", "/")
	return Partition{
		Partition: partition,
		Cluster:   splittedParts[0],
		Topic:     topicName,
	}
}

func NewEmptyPartition() Partition {
	return Partition{
		Partition: 0,
		Cluster:   "",
		Topic:     "",
	}
}
