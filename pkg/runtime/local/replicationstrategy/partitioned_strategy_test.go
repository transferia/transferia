package replicationstrategy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestPartitionedStrategy(t *testing.T) {
	cp := coordinator.NewStatefulFakeClient()
	transfer := &model.Transfer{
		ReplicationRuntime: fakeRuntime{
			workerIdx: 0,
			workerNum: 1,
		},
	}

	t.Run("NewPartitionedStrategy", func(t *testing.T) {
		partitionedStrategy, err := NewPartitionedStrategy(transfer, cp, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
		require.NoError(t, err)

		require.NotNil(t, partitionedStrategy.errCh)
		require.NotNil(t, partitionedStrategy.logger)
		require.NotNil(t, partitionedStrategy.newSource)
		require.NotNil(t, partitionedStrategy.newSink)
		require.NotNil(t, partitionedStrategy.newLister)
		require.Len(t, partitionedStrategy.partitionToRunner, 0)
	})

	t.Run("StartOneRunner", func(t *testing.T) {
		// Construct partitioned strategy and mock source
		partitionedStrategy, err := NewPartitionedStrategy(transfer, cp, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
		require.NoError(t, err)

		partition := abstract.Partition{Topic: "test", Partition: 0}
		srcData := newTestChangeItems(map[abstract.Partition][]string{partition: {"1", "2", "3"}})
		srcItemCount := totalItemCount(srcData)
		partitionedStrategy.newSource = newMockSourceFactory(srcData)
		partitionedStrategy.newLister = newMockListerFromData(srcData)

		// Run
		res, err := runPartitionedStrategyWaitItemsAndStop(partitionedStrategy, srcItemCount, 2*time.Second)

		// Assert
		require.NoError(t, err)
		require.Len(t, res, srcItemCount)
		for idx, resItem := range res {
			require.Equal(t, srcData[partition][idx].ColumnValues, resItem.ColumnValues)
		}

		require.Len(t, partitionedStrategy.partitionToRunner, 1)
	})

	t.Run("StartFewRunners", func(t *testing.T) {
		// Construct partitioned strategy and mock source
		partitionedStrategy, err := NewPartitionedStrategy(transfer, cp, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
		require.NoError(t, err)

		firstPartition := abstract.Partition{Topic: "test", Partition: 0}
		secondPartition := abstract.Partition{Topic: "test", Partition: 1}
		srcData := newTestChangeItems(map[abstract.Partition][]string{
			firstPartition:  {"1", "2", "3"},
			secondPartition: {"4", "5", "6"},
		})
		srcItemCount := totalItemCount(srcData)
		partitionedStrategy.newSource = newMockSourceFactory(srcData)
		partitionedStrategy.newLister = newMockListerFromData(srcData)

		// Run
		res, err := runPartitionedStrategyWaitItemsAndStop(partitionedStrategy, srcItemCount, 2*time.Second)

		// Assert
		require.NoError(t, err)
		require.Len(t, res, srcItemCount)

		resByPartition := make(map[string][]abstract.ChangeItem)
		for _, item := range res {
			resByPartition[item.Table] = append(resByPartition[item.Table], item)
		}
		for partition, srcItems := range srcData {
			resItems, ok := resByPartition[partition.String()]
			require.True(t, ok)
			require.Equal(t, srcItems, resItems)
		}

		require.Len(t, partitionedStrategy.partitionToRunner, 2)
	})

	t.Run("ErrorInOneRunner", func(t *testing.T) {
		// Construct partitioned strategy and mock source
		partitionedStrategy, err := NewPartitionedStrategy(transfer, cp, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
		require.NoError(t, err)

		partition := abstract.Partition{Topic: "test", Partition: 0}
		srcData := newTestChangeItems(map[abstract.Partition][]string{partition: {"1"}})
		partitionedStrategy.newSource = newMockSourceFactory(srcData)
		partitionedStrategy.newLister = newMockListerFromData(srcData)

		// Run
		partitionedStrategy.newSink = newMockQueueToS3SinkFactory(func(got []abstract.ChangeItem) error {
			return xerrors.New("some sink error")
		})

		errCh := make(chan error, 1)
		go func() {
			errCh <- partitionedStrategy.Run()
		}()

		// Assert
		require.Error(t, <-errCh)
		require.Len(t, partitionedStrategy.partitionToRunner, 1)
	})

	t.Run("ErrorInFewRunners", func(t *testing.T) {
		// Construct partitioned strategy and mock source
		partitionedStrategy, err := NewPartitionedStrategy(transfer, cp, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
		require.NoError(t, err)

		firstPartition := abstract.Partition{Topic: "test", Partition: 0}
		secondPartition := abstract.Partition{Topic: "test", Partition: 1}
		srcData := newTestChangeItems(map[abstract.Partition][]string{
			firstPartition:  {"1"},
			secondPartition: {"4"},
		})
		partitionedStrategy.newSource = newMockSourceFactory(srcData)
		partitionedStrategy.newLister = newMockListerFromData(srcData)

		// Run
		partitionedStrategy.newSink = newMockQueueToS3SinkFactory(func(got []abstract.ChangeItem) error {
			return xerrors.Errorf("some sink error for partition %s", got[0].Table)
		})

		errCh := make(chan error, 1)
		go func() {
			errCh <- partitionedStrategy.Run()
		}()
		err = <-errCh

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "{\"partition\":0,\"topic\":\"test\"}")
		require.Contains(t, err.Error(), "{\"partition\":1,\"topic\":\"test\"}")
		require.Len(t, partitionedStrategy.partitionToRunner, 2)
	})
}

func TestSyncRunnersWithPartitions(t *testing.T) {
	cp := coordinator.NewStatefulFakeClient()
	transfer := &model.Transfer{
		ReplicationRuntime: &fakeRuntime{
			workerIdx: 0,
			workerNum: 1,
		},
	}

	partitionedStrategy, err := NewPartitionedStrategy(transfer, cp, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	require.NoError(t, err)

	var partitions []abstract.Partition
	partitionedStrategy.newSource = newMockSourceFactory(nil)
	partitionedStrategy.newLister = newMockListerFactory(func() ([]abstract.Partition, error) {
		return partitions, nil
	})

	partitionedStrategy.newSink = newMockQueueToS3SinkFactory(func(_ []abstract.ChangeItem) error {
		return nil
	})

	t.Run("First", func(t *testing.T) {
		partitions = []abstract.Partition{
			{Partition: 0, Topic: "test_1"},
			{Partition: 1, Topic: "test_1"},
			{Partition: 0, Topic: "test_2"},
		}

		require.NoError(t, partitionedStrategy.syncRunnersWithPartitions())
		require.Len(t, partitionedStrategy.partitionToRunner, 3)
		for _, testPartition := range partitions {
			require.Contains(t, partitionedStrategy.partitionToRunner, testPartition)
		}
	})

	t.Run("NoChanges", func(t *testing.T) {
		partitions = []abstract.Partition{
			{Partition: 0, Topic: "test_1"},
			{Partition: 1, Topic: "test_1"},
			{Partition: 0, Topic: "test_2"},
		}

		require.NoError(t, partitionedStrategy.syncRunnersWithPartitions())
		require.Len(t, partitionedStrategy.partitionToRunner, 3)
		for _, testPartition := range partitions {
			require.Contains(t, partitionedStrategy.partitionToRunner, testPartition)
		}
	})

	t.Run("TheSameCountButDifferentPartitions", func(t *testing.T) {
		partitions = []abstract.Partition{
			{Partition: 0, Topic: "test_1"},
			{Partition: 1, Topic: "test_1"},
			{Partition: 0, Topic: "test_3"},
		}

		require.NoError(t, partitionedStrategy.syncRunnersWithPartitions())
		require.Len(t, partitionedStrategy.partitionToRunner, 3)
		for _, testPartition := range partitions {
			require.Contains(t, partitionedStrategy.partitionToRunner, testPartition)
		}
	})

	t.Run("PartitionsNumberHasIncreased", func(t *testing.T) {
		partitions = []abstract.Partition{
			{Partition: 0, Topic: "test_1"},
			{Partition: 1, Topic: "test_1"},
			{Partition: 0, Topic: "test_3"},
			{Partition: 1, Topic: "test_3"},
		}

		require.NoError(t, partitionedStrategy.syncRunnersWithPartitions())
		require.Len(t, partitionedStrategy.partitionToRunner, 4)
		for _, testPartition := range partitions {
			require.Contains(t, partitionedStrategy.partitionToRunner, testPartition)
		}
	})

	t.Run("PartitionsNumberHasDecreased", func(t *testing.T) {
		partitions = []abstract.Partition{
			{Partition: 0, Topic: "test_3"},
			{Partition: 1, Topic: "test_3"},
		}

		require.NoError(t, partitionedStrategy.syncRunnersWithPartitions())
		require.Len(t, partitionedStrategy.partitionToRunner, 2)
		for _, testPartition := range partitions {
			require.Contains(t, partitionedStrategy.partitionToRunner, testPartition)
		}
	})

	t.Run("PartitionsNumberHasDecreasedBecauseOfAssignment", func(t *testing.T) {
		partitionedStrategy.currWorkerIndex = 0
		partitionedStrategy.totalWorkersNum = 2

		partitions = []abstract.Partition{
			{Partition: 0, Topic: "test_3"},
			{Partition: 1, Topic: "test_3"},
		}

		require.NoError(t, partitionedStrategy.syncRunnersWithPartitions())
		require.Len(t, partitionedStrategy.partitionToRunner, 1)
		require.Contains(t, partitionedStrategy.partitionToRunner, partitions[0])
	})
}

func TestGetOrderedPartitions(t *testing.T) {
	cp := coordinator.NewStatefulFakeClient()
	transfer := &model.Transfer{
		ReplicationRuntime: &fakeRuntime{
			workerIdx: 0,
			workerNum: 1,
		},
	}

	partitionedStrategy, err := NewPartitionedStrategy(transfer, cp, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	require.NoError(t, err)

	var partitions []abstract.Partition
	partitionedStrategy.newSource = newMockSourceFactory(nil)
	partitionedStrategy.newLister = newMockListerFactory(func() ([]abstract.Partition, error) {
		return partitions, nil
	})

	t.Run("GetFromExistingRunner", func(t *testing.T) {
		partitions = []abstract.Partition{
			{Partition: 0, Topic: "test_1"},
			{Partition: 1, Topic: "test_1"},
		}

		testPartition := abstract.NewPartition("test_topic", 0)
		src, err := partitionedStrategy.newSource(testPartition)
		require.NoError(t, err)
		dst := mocksink.NewMockQueueToS3Sink(nil, nil, nil)
		partitionedStrategy.partitionToRunner[testPartition] = newPartitionRunner(src, dst)

		orderedPartitions, err := partitionedStrategy.getOrderedPartitions()
		require.NoError(t, err)
		require.Equal(t, partitions, orderedPartitions)
	})

	t.Run("GetFromLister", func(t *testing.T) {
		partitions = []abstract.Partition{
			{Partition: 0, Topic: "test_2"},
			{Partition: 1, Topic: "test_2"},
		}

		partitionedStrategy.partitionToRunner = make(map[abstract.Partition]*partitionRunner)

		orderedPartitions, err := partitionedStrategy.getOrderedPartitions()
		require.NoError(t, err)
		require.Equal(t, partitions, orderedPartitions)
	})

	t.Run("RecreateListerOnError", func(t *testing.T) {
		partitions = []abstract.Partition{
			{Partition: 0, Topic: "test_2"},
		}

		var listCalls int
		partitionedStrategy.partitionToRunner = make(map[abstract.Partition]*partitionRunner)
		partitionedStrategy.lister = nil
		partitionedStrategy.newLister = func() (abstract.PartitionLister, error) {
			return &listerMock{listPartitionsF: func() ([]abstract.Partition, error) {
				listCalls++
				if listCalls == 1 {
					return nil, xerrors.New("temporary list error")
				}
				return partitions, nil
			}}, nil
		}

		orderedPartitions, err := partitionedStrategy.getOrderedPartitions()
		require.NoError(t, err)
		require.Equal(t, partitions, orderedPartitions)
		require.Equal(t, 2, listCalls)
	})

	t.Run("GetOrderedPartitions", func(t *testing.T) {
		partitions = []abstract.Partition{
			{Partition: 3, Topic: "test_3"},
			{Partition: 1, Topic: "test_2"},
			{Partition: 0, Topic: "test_2"},
		}
		expectedPartitions := []abstract.Partition{
			{Partition: 0, Topic: "test_2"},
			{Partition: 1, Topic: "test_2"},
			{Partition: 3, Topic: "test_3"},
		}

		orderedPartitions, err := partitionedStrategy.getOrderedPartitions()
		require.NoError(t, err)
		require.Equal(t, expectedPartitions, orderedPartitions)
	})
}

func TestAssignedPartitions(t *testing.T) {
	allPartitions := []abstract.Partition{
		{Partition: 0, Topic: "test"},
		{Partition: 1, Topic: "test"},
		{Partition: 2, Topic: "test"},
		{Partition: 3, Topic: "test"},
		{Partition: 4, Topic: "test"},
	}

	t.Run("ZeroIndexOneWorker", func(t *testing.T) {
		expectedPartitions := allPartitions
		res := assignedPartitions(0, 1, allPartitions)

		require.Equal(t, expectedPartitions, res)
	})

	t.Run("ZeroIndexTwoWorkers", func(t *testing.T) {
		expectedPartitions := []abstract.Partition{
			{Partition: 0, Topic: "test"},
			{Partition: 2, Topic: "test"},
			{Partition: 4, Topic: "test"},
		}

		res := assignedPartitions(0, 2, allPartitions)
		require.Equal(t, expectedPartitions, res)
	})

	t.Run("FirstIndexTwoWorkers", func(t *testing.T) {
		expectedPartitions := []abstract.Partition{
			{Partition: 1, Topic: "test"},
			{Partition: 3, Topic: "test"},
		}

		res := assignedPartitions(1, 2, allPartitions)
		require.Equal(t, expectedPartitions, res)
	})

	t.Run("MoreWorkersThanPartitions", func(t *testing.T) {
		expectedPartitions := make([]abstract.Partition, 0)

		res := assignedPartitions(5, 10, allPartitions)
		require.Equal(t, expectedPartitions, res)
	})

	t.Run("EmptyPartitions", func(t *testing.T) {
		expectedPartitions := make([]abstract.Partition, 0)

		res := assignedPartitions(0, 1, []abstract.Partition{})
		require.Equal(t, expectedPartitions, res)
	})

	fewTopicsAllPartitions := []abstract.Partition{
		{Partition: 0, Topic: "test_1"},
		{Partition: 1, Topic: "test_1"},
		{Partition: 2, Topic: "test_1"},
		{Partition: 3, Topic: "test_1"},
		{Partition: 4, Topic: "test_1"},
		{Partition: 0, Topic: "test_2"},
		{Partition: 1, Topic: "test_2"},
		{Partition: 2, Topic: "test_2"},
	}

	t.Run("FewTopicsZeroIndexTwoWorkers", func(t *testing.T) {
		expectedPartitions := []abstract.Partition{
			{Partition: 0, Topic: "test_1"},
			{Partition: 2, Topic: "test_1"},
			{Partition: 4, Topic: "test_1"},
			{Partition: 0, Topic: "test_2"},
			{Partition: 2, Topic: "test_2"},
		}

		res := assignedPartitions(0, 2, fewTopicsAllPartitions)
		require.Equal(t, expectedPartitions, res)
	})

	t.Run("FewTopicsMoreWorkersThanPartitions", func(t *testing.T) {
		expectedPartitions := []abstract.Partition{
			{Partition: 4, Topic: "test_1"},
		}

		res := assignedPartitions(4, 10, fewTopicsAllPartitions)
		require.Equal(t, expectedPartitions, res)
	})
}

type fakeRuntime struct {
	workerIdx, workerNum int
}

func (r fakeRuntime) NeedRestart(_ abstract.Runtime) bool {
	return false
}

func (r fakeRuntime) WithDefaults() {}

func (r fakeRuntime) Validate() error {
	return nil
}

func (r fakeRuntime) Type() abstract.RuntimeType {
	return abstract.LocalRuntimeType
}

func (r fakeRuntime) SetVersion(_ string, _ *string) error {
	return nil
}

func (r fakeRuntime) IsSupportedPartitionedStrategy() bool {
	return true
}

func (r fakeRuntime) CurrentJobIndex() int {
	return r.workerIdx
}

func (r fakeRuntime) SnapshotWorkersNum() int {
	return -1
}

func (r fakeRuntime) SnapshotThreadsNumPerWorker() int {
	return -1
}

func (r fakeRuntime) SnapshotIsMain() bool {
	return false
}

func (r fakeRuntime) ReplicationWorkersNum() int {
	return r.workerNum
}

type sourceMock struct {
	ctx    context.Context
	cancel context.CancelFunc

	part abstract.Partition
	data map[abstract.Partition][]abstract.ChangeItem
}

func (m *sourceMock) Run(sink abstract.QueueToS3Sink) error {
	errCh := make(chan abstract.AsyncPushResult, 1)
	for _, item := range m.data[m.part] {
		sink.AsyncV2Push(context.Background(), errCh, []abstract.ChangeItem{item})
	}

	select {
	case err := <-errCh:
		return err.GetError()
	default:
	}

	<-m.ctx.Done()
	return nil
}

func (m *sourceMock) Stop() {
	m.cancel()
}

type listerMock struct {
	listPartitionsF func() ([]abstract.Partition, error)
	closed          bool
}

func (m *listerMock) ListPartitions() ([]abstract.Partition, error) {
	if m.listPartitionsF != nil {
		return m.listPartitionsF()
	}

	return nil, nil
}

func (m *listerMock) Close() {
	m.closed = true
}

func newMockListerFromData(data map[abstract.Partition][]abstract.ChangeItem) newListerF {
	return newMockListerFactory(func() ([]abstract.Partition, error) {
		partitions := make([]abstract.Partition, 0, len(data))
		for partition := range data {
			partitions = append(partitions, partition)
		}

		return partitions, nil
	})
}

func newMockListerFactory(listPartitionsF func() ([]abstract.Partition, error)) newListerF {
	return func() (abstract.PartitionLister, error) {
		return &listerMock{listPartitionsF: listPartitionsF}, nil
	}
}

func newMockSourceFactory(data map[abstract.Partition][]abstract.ChangeItem) newSourceF {
	return func(partition abstract.Partition) (abstract.QueueToS3Source, error) {
		ctx, cancel := context.WithCancel(context.Background())
		return &sourceMock{
			ctx:    ctx,
			cancel: cancel,
			part:   partition,
			data:   data,
		}, nil
	}
}

func newMockQueueToS3SinkFactory(callback func([]abstract.ChangeItem) error) newSinkF {
	return func() (abstract.QueueToS3Sink, error) {
		return mocksink.NewMockQueueToS3Sink(
			func(item abstract.ChangeItem) uint64 {
				return 0
			},
			callback,
			func(offsets []uint64) bool {
				return false
			},
		), nil
	}
}

func newTestChangeItems(data map[abstract.Partition][]string) map[abstract.Partition][]abstract.ChangeItem {
	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{DataType: schema.TypeString.String(), ColumnName: "MyString"},
	})

	result := make(map[abstract.Partition][]abstract.ChangeItem)
	for partition, val := range data {
		result[partition] = append(result[partition], abstract.ChangeItem{
			TableSchema:  tableSchema,
			Kind:         abstract.InsertKind,
			Schema:       "test_namespace",
			Table:        partition.String(),
			ColumnNames:  tableSchema.ColumnNames(),
			ColumnValues: []any{val},
		})
	}

	return result
}

func totalItemCount(data map[abstract.Partition][]abstract.ChangeItem) int {
	var res int
	for _, items := range data {
		res += len(items)
	}
	return res
}

func runPartitionedStrategyWaitItemsAndStop(
	partitionedStrategy *PartitionedStrategy,
	desiredCount int,
	timeout time.Duration,
) ([]abstract.ChangeItem, error) {
	mu := sync.Mutex{}
	var res []abstract.ChangeItem

	partitionedStrategy.newSink = newMockQueueToS3SinkFactory(func(got []abstract.ChangeItem) error {
		mu.Lock()
		res = append(res, got...)
		mu.Unlock()
		return nil
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- partitionedStrategy.Run()
	}()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	wasErrorReceived := false
	var strategyRunErr error
	stillWaiting := true
	for stillWaiting {
		if len(res) >= desiredCount {
			break
		}

		select {
		case err := <-errCh:
			strategyRunErr = err
			wasErrorReceived = true
			stillWaiting = false
		case <-deadline.C:
			stillWaiting = false
		case <-time.After(2 * time.Millisecond):
			// Small backoff to avoid busy spinning.
		}
	}

	if err := partitionedStrategy.Stop(); err != nil {
		return nil, err
	}
	if !wasErrorReceived {
		strategyRunErr = <-errCh
	}

	return res, strategyRunErr
}
