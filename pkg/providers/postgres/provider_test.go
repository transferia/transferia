package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
)

func TestSnapshotTurnOffPerTransactionPush(t *testing.T) {
	dst := &PgDestination{
		Hosts: []string{
			"localhost:0",
		},
		PerTransactionPush: true,
	}

	transfer := new(model.Transfer)
	transfer.Dst = dst

	provider := New(logger.Log, metrics.NewRegistry(), nil, transfer).(*Provider)

	_, _ = provider.SnapshotSink(middlewares.Config{})
	require.False(t, transfer.Dst.(*PgDestination).PerTransactionPush)
}
