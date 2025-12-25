package abstract

type FakeShardingTaskRuntime struct {
	currentJobIndex             int
	snapshotWorkersNum          int
	snapshotThreadsNumPerWorker int
	replicationWorkersNum       int
}

func (r *FakeShardingTaskRuntime) SnapshotWorkersNum() int {
	return r.snapshotWorkersNum
}

func (r *FakeShardingTaskRuntime) ReplicationWorkersNum() int {
	return r.replicationWorkersNum
}

func (r *FakeShardingTaskRuntime) SnapshotThreadsNumPerWorker() int {
	return r.snapshotThreadsNumPerWorker
}

func (r *FakeShardingTaskRuntime) CurrentJobIndex() int {
	return r.currentJobIndex
}

func (r *FakeShardingTaskRuntime) SnapshotIsMain() bool {
	return r.currentJobIndex == 0
}

func NewFakeShardingTaskRuntime(
	currentJobIndex int,
	snapshotWorkersNum int,
	snapshotThreadsNumPerWorker int,
	replicationWorkersNum int,
) *FakeShardingTaskRuntime {
	return &FakeShardingTaskRuntime{
		currentJobIndex:             currentJobIndex,
		snapshotWorkersNum:          snapshotWorkersNum,
		snapshotThreadsNumPerWorker: snapshotThreadsNumPerWorker,
		replicationWorkersNum:       replicationWorkersNum,
	}
}
