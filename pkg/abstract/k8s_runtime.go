package abstract

var _ Runtime = (*K8sRuntime)(nil)
var _ ShardingTaskRuntime = (*K8sRuntime)(nil)

type K8sRuntime struct {
	ImageId       string
	TotalJobCount int
	JobIndex      int
	Threads       int
	IsMainJob     bool
}

func (r *K8sRuntime) CurrentJobIndex() int {
	return r.JobIndex // TODO: TM-10237
}

func (r *K8sRuntime) SnapshotWorkersNum() int {
	return r.TotalJobCount // TODO: TM-10237
}

func (r *K8sRuntime) SnapshotThreadsNumPerWorker() int {
	return r.Threads
}

func (r *K8sRuntime) SnapshotIsMain() bool {
	return r.IsMainJob // TODO: TM-10237
}

func (r *K8sRuntime) ReplicationWorkersNum() int {
	return r.TotalJobCount // TODO: TM-10237
}

func (r *K8sRuntime) NeedRestart(runtime Runtime) bool {
	k8sRuntime, ok := runtime.(*K8sRuntime) // TODO: TM-10238
	if !ok {
		return false
	}
	return r.ImageId != k8sRuntime.ImageId
}

func (r *K8sRuntime) WithDefaults() {
	if r.TotalJobCount == 0 {
		r.TotalJobCount = 1
	}
}

func (r *K8sRuntime) CopyWithDefaults() *K8sRuntime {
	copied := *r
	copied.WithDefaults()
	return &copied
}

func (r *K8sRuntime) Validate() error {
	return nil // TODO: TM-10238
}

func (r *K8sRuntime) Type() RuntimeType {
	return K8sRuntimeType
}

func (r *K8sRuntime) SetVersion(runtimeSpecificVersion string, _ *string) error {
	r.ImageId = runtimeSpecificVersion // TODO: TM-10238
	return nil
}

func (r *K8sRuntime) IsSupportedPartitionedStrategy() bool {
	return true // TODO: TM-10237
}
