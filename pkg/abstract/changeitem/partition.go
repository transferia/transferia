package changeitem

import (
	"fmt"
	"strings"
)

type Partition struct {
	Cluster   string `json:"cluster,omitempty"`
	Partition uint32 `json:"partition"`
	Topic     string `json:"topic"`
}

func (p Partition) String() string {
	if p.Cluster != "" {
		return fmt.Sprintf("{\"cluster\":\"%s\",\"partition\":%d,\"topic\":\"%s\"}", p.Cluster, p.Partition, p.Topic)
	}
	return fmt.Sprintf("{\"partition\":%d,\"topic\":\"%s\"}", p.Partition, p.Topic)
}

// LegacyLogbrokerPqv0String is only necessary for the logfeller parser, because
// the .cpp code has specific logic for parsing transport meta in such format.
func (p Partition) LegacyLogbrokerPqv0String() string {
	slashes := strings.Count(p.Topic, "/")
	oldFashionTopic := strings.ReplaceAll(strings.Replace(p.Topic, "/", "@", slashes-1), "/", "--")

	return fmt.Sprintf("rt3.%s--%s:%v", p.Cluster, oldFashionTopic, p.Partition)
}

func NewPartition(topic string, partition uint32) Partition {
	return Partition{
		Cluster:   "",
		Partition: partition,
		Topic:     topic,
	}
}

func NewPartitionWithCluster(topic string, partition uint32, cluster string) Partition {
	return Partition{
		Cluster:   cluster,
		Partition: partition,
		Topic:     topic,
	}
}

func NewEmptyPartition() Partition {
	return Partition{
		Cluster:   "",
		Partition: 0,
		Topic:     "",
	}
}
