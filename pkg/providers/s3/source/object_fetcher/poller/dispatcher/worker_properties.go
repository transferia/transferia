//go:build !disable_s3_provider

package dispatcher

import "github.com/transferia/transferia/library/go/core/xerrors"

type WorkerProperties struct {
	currentWorkerNum int
	totalWorkersNum  int
}

func (p *WorkerProperties) CurrentWorkerNum() int {
	return p.currentWorkerNum
}

func (p *WorkerProperties) TotalWorkersNum() int {
	return p.totalWorkersNum
}

func NewWorkerProperties(currentWorkerNum int, totalWorkersNum int) (*WorkerProperties, error) {
	if currentWorkerNum >= totalWorkersNum {
		return nil, xerrors.New("currentWorkerNum cannot be greater or equal than totalWorkersNum")
	}
	return &WorkerProperties{
		currentWorkerNum: currentWorkerNum,
		totalWorkersNum:  totalWorkersNum,
	}, nil
}
