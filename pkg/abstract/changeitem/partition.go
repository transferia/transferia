package changeitem

import (
	"fmt"
	"strings"
)

type Partition struct {
	Partition uint32 `json:"partition"`
	Topic     string `json:"topic"`
}

func (p Partition) String() string {
	return fmt.Sprintf("{\"partition\":%d,\"topic\":\"%s\"}", p.Partition, p.Topic)
}

// LegacyLogbrokerPqv0String is only necessary for the logfeller parser, because
// the .cpp code has specific logic for parsing transport meta in such format.
func (p Partition) LegacyLogbrokerPqv0String() string {
	slashes := strings.Count(p.Topic, "/")
	oldFashionTopic := strings.ReplaceAll(strings.Replace(p.Topic, "/", "@", slashes-1), "/", "--")

	cluster := "" // cluster is always empty for new reading protocols
	return fmt.Sprintf("rt3.%s--%s:%v", cluster, oldFashionTopic, p.Partition)
}

func NewPartition(topic string, partition uint32) Partition {
	return Partition{
		Partition: partition,
		Topic:     topic,
	}
}

func NewEmptyPartition() Partition {
	return Partition{
		Partition: 0,
		Topic:     "",
	}
}
