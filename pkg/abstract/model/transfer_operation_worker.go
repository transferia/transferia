package model

type OperationWorker struct {
	OperationID string
	WorkerIndex int
	Completed   bool
	Err         string
	Code        string
	Categories  []string
	Progress    *AggregatedProgress
}

func NewOperationWorker() *OperationWorker {
	return &OperationWorker{
		OperationID: "",
		WorkerIndex: 0,
		Completed:   false,
		Err:         "",
		Code:        "",
		Categories:  nil,
		Progress:    nil,
	}
}
