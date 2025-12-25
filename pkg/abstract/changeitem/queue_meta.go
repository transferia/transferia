package changeitem

type QueueMessageMeta struct {
	TopicName    string
	PartitionNum int

	// kafka - offset is 'long' (signed int64) - https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.Offset.html
	// yds - offset if uint64
	Offset uint64
	Index  int
}
