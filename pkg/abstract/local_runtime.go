package abstract

type LocalRuntime struct {
	Host           string
	CurrentJob     int
	ShardingUpload ShardUploadParams
}

func (*LocalRuntime) Type() RuntimeType {
	return LocalRuntimeType
}

func (l *LocalRuntime) isRuntime() {
}

func (l *LocalRuntime) NeedRestart(runtime Runtime) bool {
	return false
}

func (l *LocalRuntime) WithDefaults() {
}

func (l *LocalRuntime) Validate() error {
	return nil
}

func (l *LocalRuntime) isShardingEnabled()               {}
func (l *LocalRuntime) SnapshotWorkersNum() int          { return l.ShardingUpload.JobCount }
func (l *LocalRuntime) ReplicationWorkersNum() int       { return 1 }
func (l *LocalRuntime) SnapshotThreadsNumPerWorker() int { return l.ShardingUpload.ProcessCount }
func (l *LocalRuntime) CurrentJobIndex() int             { return l.CurrentJob }
func (l *LocalRuntime) SnapshotIsMain() bool             { return l.CurrentJob == 0 }
func (l *LocalRuntime) SetVersion(runtimeSpecificVersion string, versionProperties *string) error {
	return nil
}
