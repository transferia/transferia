//go:build !arcadia
// +build !arcadia

package metering

import (
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract/model"
)

func Agent() MeteringAgent {
	commonAgentMu.Lock()
	defer commonAgentMu.Unlock()
	return NewStubAgent(logger.Log)
}

func InitializeWithTags(transfer *model.Transfer, task *model.TransferOperation, runtimeTags map[string]interface{}) {
	return
}

func WithAgent(agent MeteringAgent) MeteringAgent {
	commonAgentMu.Lock()
	defer commonAgentMu.Unlock()
	commonAgent = agent
	return commonAgent
}

func Initialize(transfer *model.Transfer, task *model.TransferOperation) {
	InitializeWithTags(transfer, task, map[string]interface{}{})
}
