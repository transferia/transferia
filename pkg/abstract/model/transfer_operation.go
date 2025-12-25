package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
)

type TransferOperation struct {
	OperationID string
	TransferID  string
	TaskType    abstract.TaskType
	Status      TaskStatus
	Params      any // TODO: interface?
	Runtime     abstract.Runtime
	Progress    *AggregatedProgress
	Author      string
	PingedAt    time.Time
}

type OperationWorkflow interface {
	OnStart(task *TransferOperation) error
	OnDone(task *TransferOperation) error
	OnError(task *TransferOperation, err error) error
}

func (t *TransferOperation) IsDone() bool {
	return t.Status.IsFinal()
}

func (t *TransferOperation) AggregatedProgress() *AggregatedProgress {
	if t.Progress != nil {
		return t.Progress
	}
	return NewAggregatedProgress()
}

func (t *TransferOperation) String() string {
	return t.TaskType.String()
}

func (t *TransferOperation) NormalisedID() string {
	id := strings.ToLower(fmt.Sprintf("%v-%v", t.TaskType.String(), t.TransferID))
	id = strings.ReplaceAll(id, "_", "")
	id = strings.ReplaceAll(id, ".", "")
	if t.TransferID == "" {
		id += t.OperationID
	}
	return id
}

type TaskStatus string

const (
	NewTask       = TaskStatus("New")
	ScheduledTask = TaskStatus("Scheduled")
	RunningTask   = TaskStatus("Running")
	CompletedTask = TaskStatus("Completed")
	FailedTask    = TaskStatus("Failed")
)

func (s TaskStatus) IsFinal() bool {
	return s == CompletedTask || s == FailedTask
}

func (s TaskStatus) IsSuccess() bool {
	return s == CompletedTask
}
