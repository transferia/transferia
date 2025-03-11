package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/internal/metrics"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/middlewares"
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
