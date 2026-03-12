package abstract

type QueueToS3Source interface {
	Run(sink QueueToS3Sink) error
	Stop()
}
