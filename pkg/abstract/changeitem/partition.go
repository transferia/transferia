package changeitem

import (
	"fmt"
)

type Partition struct {
	Partition uint32 `json:"partition"`
	Topic     string `json:"topic"`
}

func (p Partition) String() string {
	return fmt.Sprintf("{\"partition\":%d,\"topic\":\"%s\"}", p.Partition, p.Topic)
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
