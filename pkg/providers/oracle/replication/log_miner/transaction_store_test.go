package log_miner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/providers/oracle/common"
)

func startTransaction(t *testing.T, store *transactionStore, startSCN uint64) {
	startPosition, err := common.NewLogPosition(startSCN, nil, nil, common.PositionReplication, time.Time{})
	require.NoError(t, err)

	err = store.StartTransaction(fmt.Sprintf("%v", startSCN), startPosition)
	require.NoError(t, err)
}

func finishTransaction(t *testing.T, store *transactionStore, startSCN uint64, finishSCN uint64) *FinishedTransaction {
	startPosition, err := common.NewLogPosition(finishSCN, nil, nil, common.PositionReplication, time.Time{})
	require.NoError(t, err)

	transaction, err := store.FinishTransaction(fmt.Sprintf("%v", startSCN), startPosition)
	require.NoError(t, err)

	return transaction
}

func TestTransactionStore1(t *testing.T) {
	store := newTransactionStore(context.Background(), logger.Log)
	startTransaction(t, store, 1)
	transaction := finishTransaction(t, store, 1, 2)
	require.Equal(t, uint64(2), transaction.ProgressPosition.SCN())
}

func TestTransactionStore2(t *testing.T) {
	store := newTransactionStore(context.Background(), logger.Log)
	startTransaction(t, store, 1)
	startTransaction(t, store, 2)
	startTransaction(t, store, 3)
	transaction := finishTransaction(t, store, 3, 4)
	require.Equal(t, (*common.LogPosition)(nil), transaction.ProgressPosition)
}

func TestTransactionStore3(t *testing.T) {
	store := newTransactionStore(context.Background(), logger.Log)
	startTransaction(t, store, 1)
	startTransaction(t, store, 2)
	startTransaction(t, store, 3)
	require.Equal(t, 3, store.starts.Len())
	finishTransaction(t, store, 3, 4)
	require.Equal(t, 3, store.starts.Len())
	finishTransaction(t, store, 2, 5)
	require.Equal(t, 2, store.starts.Len())
	transaction := finishTransaction(t, store, 1, 6)
	require.Equal(t, uint64(6), transaction.ProgressPosition.SCN())
}

func TestTransactionStore4(t *testing.T) {
	store := newTransactionStore(context.Background(), logger.Log)
	startTransaction(t, store, 0)
	startTransaction(t, store, 1)
	startTransaction(t, store, 2)
	finishTransaction(t, store, 2, 3)
	startTransaction(t, store, 4)
	finishTransaction(t, store, 4, 5)
	startTransaction(t, store, 6)
	finishTransaction(t, store, 6, 7)
	require.Equal(t, 3, store.starts.Len())
	transaction := finishTransaction(t, store, 0, 8)
	require.Equal(t, uint64(0), transaction.ProgressPosition.SCN())
}

func TestTransactionStore5(t *testing.T) {
	store := newTransactionStore(context.Background(), logger.Log)
	startTransaction(t, store, 1)
	startTransaction(t, store, 10)
	finishTransaction(t, store, 10, 11)
	startTransaction(t, store, 20)
	finishTransaction(t, store, 20, 21)
	startTransaction(t, store, 30)
	finishTransaction(t, store, 30, 31)
	startTransaction(t, store, 100)
	require.Equal(t, 3, store.starts.Len())
	transaction := finishTransaction(t, store, 1, 200)
	require.Equal(t, uint64(30), transaction.ProgressPosition.SCN())
}
