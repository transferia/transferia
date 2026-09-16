package delivery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
)

func Deactivate(t *testing.T, transfer *model.Transfer, worker *Worker) {
	require.NoError(t, tasks.Deactivate(context.Background(), worker.cp, *transfer, model.TransferOperation{}, testmetrics.EmptyRegistry()))
}
