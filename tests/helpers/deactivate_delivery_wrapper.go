package helpers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/worker/tasks"
)

func Deactivate(t *testing.T, transfer *model.Transfer, worker *Worker, onErrorCallback ...func(err error)) error {
	if len(onErrorCallback) == 0 {
		// append default callback checker: no error!
		onErrorCallback = append(onErrorCallback, func(err error) {
			require.NoError(t, err)
		})
	}
	return DeactivateErr(transfer, worker, onErrorCallback...)
}

func DeactivateErr(transfer *model.Transfer, worker *Worker, onErrorCallback ...func(err error)) error {
	return tasks.Deactivate(context.Background(), worker.cp, *transfer, model.TransferOperation{}, EmptyRegistry())
}
