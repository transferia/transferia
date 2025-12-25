package abstract

type Source interface {
	Run(sink AsyncSink) error
	Stop()
}

type Fetchable interface {
	Fetch() ([]ChangeItem, error)
}
