package source

import "github.com/transferia/transferia/pkg/parsers"

type commitFunc func()

type committableBatch struct {
	Batches []parsers.MessageBatch
	commitF commitFunc
}

func (b committableBatch) Commit() {
	if b.commitF != nil {
		b.commitF()
	}
}

func newBatch(commitF commitFunc, batches []parsers.MessageBatch) committableBatch {
	return committableBatch{
		Batches: batches,
		commitF: commitF,
	}
}

func newEmtpyBatch() committableBatch {
	return committableBatch{
		Batches: []parsers.MessageBatch{},
		commitF: nil,
	}
}
