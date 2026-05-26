package abstract

type Source interface {
	Run(sink AsyncSink) error
	Stop()
}

type Fetchable interface {
	Fetch() ([]ChangeItem, error)
}

type PartitionLister interface {
	ListPartitions() ([]Partition, error)
	Close()
}
