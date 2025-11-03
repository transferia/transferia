//go:build !arcadia
// +build !arcadia

package metering

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func Agent() MeteringAgent {
	commonAgentMu.Lock()
	defer commonAgentMu.Unlock()
	return NewStubAgent(logger.Log)
}

func InitializeWithTags(transfer *model.Transfer, task *model.TransferOperation, runtimeTags map[string]interface{}, meteringStats *MeteringStats) {
	return
}

func NewMeteringStats(registry metrics.Registry) *MeteringStats {
	return &MeteringStats{}
}

func WithAgent(agent MeteringAgent) MeteringAgent {
	commonAgentMu.Lock()
	defer commonAgentMu.Unlock()
	commonAgent = agent
	return commonAgent
}

func Initialize(transfer *model.Transfer, task *model.TransferOperation) {
	InitializeWithTags(transfer, task, map[string]interface{}{}, nil)
}
