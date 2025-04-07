package protocol

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/providers/delta/action"
	"github.com/transferia/transferia/pkg/providers/delta/store"
	"github.com/transferia/transferia/pkg/util/set"
)

type SnapshotReader struct {
	logStore         store.Store
	checkpointReader CheckpointReader
	history          *history

	mu              *sync.Mutex
	currentSnapshot atomic.Pointer[Snapshot]
}

func NewSnapshotReader(cpReader CheckpointReader, logStore store.Store, history *history) (*SnapshotReader, error) {
	s := &SnapshotReader{
		logStore:         logStore,
		checkpointReader: cpReader,
		history:          history,
		mu:               &sync.Mutex{},
		currentSnapshot:  atomic.Pointer[Snapshot]{},
	}

	initSnapshot, err := s.atInit()
	if err != nil {
		return nil, xerrors.Errorf("unable to init snapshot from start: %w", err)
	}

	// load it as an atomic reference
	s.currentSnapshot.Store(initSnapshot)

	return s, nil
}

func (r *SnapshotReader) minRetentionTS() (int64, error) {
	var metadata *action.Metadata
	var err error

	if r.snapshot() == nil {
		metadata = new(action.Metadata)
	} else {
		metadata, err = r.snapshot().Metadata()
	}

	if err != nil {
		return 0, xerrors.Errorf("unable to get snapshot meta: %w", err)
	}
	// in milliseconds
	tombstoneRetention, err := TombstoneRetentionProp.fromMetadata(metadata)
	if err != nil {
		return 0, xerrors.Errorf("unable to get retention from table meta: %w", err)
	}
	return time.Now().UnixMilli() - tombstoneRetention.Milliseconds(), nil
}

func (r *SnapshotReader) snapshot() *Snapshot {
	return r.currentSnapshot.Load()
}

func (r *SnapshotReader) atInit() (*Snapshot, error) {
	lastCheckpoint, err := LastCheckpoint(r.logStore)
	if err != nil {
		return nil, xerrors.Errorf("last checkpoint: %w", err)
	}

	ver := int64(-1)
	if lastCheckpoint != nil {
		ver = lastCheckpoint.Version
	}
	logSegment, err := r.logSegmentForVersion(ver, -1)
	if err != nil {
		if xerrors.Is(err, store.ErrFileNotFound) {
			return NewInitialSnapshot(r.logStore.Root(), r.logStore, r.checkpointReader)
		}
		return nil, xerrors.Errorf("unable to read all log segments: %w", err)
	}

	res, err := r.createSnapshot(logSegment, logSegment.LastCommitTS.UnixMilli())
	if err != nil {
		return nil, xerrors.Errorf("unable to init snapshot at: %v: %w", logSegment.LastCommitTS, err)
	}
	return res, nil
}

func (r *SnapshotReader) atVersion(version int64) (*Snapshot, error) {
	if r.snapshot().Version() == version {
		return r.snapshot(), nil
	}

	startingCheckpoint, err := FindLastCompleteCheckpoint(r.logStore, CheckpointInstance{Version: version, NumParts: -1})
	if err != nil {
		return nil, xerrors.Errorf("unable to find last checkpoint for version: %v: %w", version, err)
	}

	start := int64(-1)
	if startingCheckpoint != nil {
		start = startingCheckpoint.Version
	}
	segment, err := r.logSegmentForVersion(start, version)
	if err != nil {
		return nil, xerrors.Errorf("unable to get log segment for version: %v: %w", version, err)
	}

	return r.createSnapshot(segment, segment.LastCommitTS.UnixMilli())
}

func (r *SnapshotReader) forVersion(version int64) (*Snapshot, error) {
	if err := r.history.checkVersionExists(version, r); err != nil {
		return nil, xerrors.Errorf("unable to check version: %v exist: %w", version, err)
	}
	return r.atVersion(version)
}

func (r *SnapshotReader) forTimestamp(timestamp int64) (*Snapshot, error) {
	latestCommit, err := r.history.activeCommitAtTime(r, timestamp, false, true, false)
	if err != nil {
		return nil, xerrors.Errorf("unable to find active commit at: %v: %w", timestamp, err)
	}
	return r.atVersion(latestCommit.version)
}

func (r *SnapshotReader) logSegmentForVersion(startCheckpoint int64, versionToLoad int64) (*LogSegment, error) {
	prefix := CheckpointPrefix(r.logStore.Root()+"/_delta_log/", startCheckpoint)
	iter, err := r.logStore.ListFrom(prefix)
	if err != nil {
		return nil, xerrors.Errorf("unable to list prefix: %s: %w", prefix, err)
	}
	defer iter.Close()

	var newFiles []*store.FileMeta
	// List from the starting  If a checkpoint doesn't exist, this will still return
	// deltaVersion=0.
	for iter.Next() {
		f, err := iter.Value()
		if err != nil {
			return nil, xerrors.Errorf("unable to load row: %w", err)
		}
		if !(IsCheckpointFile(f.Path()) || IsDeltaFile(f.Path())) {
			continue
		}
		if IsCheckpointFile(f.Path()) && f.Size() == 0 {
			continue
		}
		v, err := GetFileVersion(f.Path())
		if err != nil {
			continue
		}
		if versionToLoad <= 0 || (versionToLoad > 0 && v <= versionToLoad) {
			newFiles = append(newFiles, f)
		} else {
			break
		}
	}

	if len(newFiles) == 0 && startCheckpoint <= 0 {
		return nil, xerrors.Errorf("empty dir: %s", r.logStore.Root())
	} else if len(newFiles) == 0 {
		// The directory may be deleted and recreated and we may have stale state in our DeltaLog
		// singleton, so try listing from the first version
		res, err := r.logSegmentForVersion(-1, versionToLoad)
		if err != nil {
			return nil, xerrors.Errorf("unable to build log segment till: %v: %w", versionToLoad, err)
		}
		return res, nil
	}

	deltas := yslices.Filter(newFiles, func(meta *store.FileMeta) bool {
		return !IsCheckpointFile(meta.Path())
	})
	checkpoints := yslices.Filter(newFiles, func(meta *store.FileMeta) bool {
		return IsCheckpointFile(meta.Path())
	})

	var lastCheckpoint CheckpointInstance
	if versionToLoad <= 0 {
		lastCheckpoint = MaxInstance
	} else {
		lastCheckpoint = CheckpointInstance{
			Version:  versionToLoad,
			NumParts: 0,
		}
	}

	checkpointFiles := yslices.Map(checkpoints, func(f *store.FileMeta) *CheckpointInstance {
		cp, _ := FromPath(f.Path()) // bad files will filter out later
		return cp
	})
	checkpointFiles = yslices.Filter(checkpointFiles, func(instance *CheckpointInstance) bool {
		return instance != nil
	})

	latesCompletedCheckpint := LatestCompleteCheckpoint(checkpointFiles, lastCheckpoint)
	if latesCompletedCheckpint != nil && latesCompletedCheckpint.Version > 0 {
		res, err := r.segmentFromCheckpoint(latesCompletedCheckpint, deltas, versionToLoad, checkpoints)
		if err != nil {
			return nil, xerrors.Errorf("unable to get checkpoint: %w", err)
		}
		return res, nil
	}
	res, err := r.emptySegment(startCheckpoint, deltas)
	if err != nil {
		return nil, xerrors.Errorf("unable to build empty segment: %w", err)
	}
	return res, nil
}

// emptySegment means there is no starting checkpoint found. This means that we should definitely have version 0, or the
// last checkpoint we thought should exist (the `_last_checkpoint` file) no longer exists
func (r *SnapshotReader) emptySegment(startCheckpoint int64, deltas []*store.FileMeta) (*LogSegment, error) {
	if startCheckpoint > 0 {
		return nil, xerrors.Errorf("missing file part: %v", startCheckpoint)
	}

	deltaVersions := yslices.Map(deltas, func(f *store.FileMeta) int64 {
		ver, _ := LogVersion(f.Path()) // bad deltas would got 0 version (i.e. no version).
		return ver
	})

	if err := verifyVersions(deltaVersions); err != nil {
		return nil, err
	}

	latestCommit := deltas[len(deltas)-1]
	lastVer, _ := LogVersion(latestCommit.Path()) // latest commit can be empty ver, so ignore parse error
	return &LogSegment{
		LogPath:           r.logStore.Root(),
		Version:           lastVer,
		Deltas:            deltas,
		Checkpoints:       nil,
		CheckpointVersion: -1,
		LastCommitTS:      latestCommit.TimeModified(),
	}, nil
}

func (r *SnapshotReader) segmentFromCheckpoint(
	latestCheckpoint *CheckpointInstance,
	deltas []*store.FileMeta,
	versionToLoad int64,
	checkpoints []*store.FileMeta,
) (*LogSegment, error) {
	newCheckpointVersion := latestCheckpoint.Version
	newCheckpointPaths := set.New(latestCheckpoint.GetCorrespondingFiles(r.logStore.Root())...)

	deltasAfterCheckpoint := yslices.Filter(deltas, func(f *store.FileMeta) bool {
		ver, err := LogVersion(f.Path())
		if err != nil {
			return false
		}
		return ver > newCheckpointVersion
	})

	deltaVersions := yslices.Map(deltasAfterCheckpoint, func(f *store.FileMeta) int64 {
		ver, _ := LogVersion(f.Path()) // err is impossible here
		return ver
	})

	if len(deltaVersions) != 0 {
		if err := verifyVersions(deltaVersions); err != nil {
			return nil, xerrors.Errorf("found invalid version: %w", err)
		}
		if deltaVersions[0] != newCheckpointVersion+1 {
			return nil, xerrors.New("unable to get the first delta to compute Snapshot")
		}
		if versionToLoad > 0 && versionToLoad == deltaVersions[len(deltaVersions)-1] {
			return nil, xerrors.New("unable to get the last delta to compute Snapshot")
		}
	}

	var newVersion int64
	if len(deltaVersions) != 0 {
		newVersion = deltaVersions[len(deltaVersions)-1]
	} else {
		newVersion = latestCheckpoint.Version
	}

	newCheckpointFiles := yslices.Filter(checkpoints, func(f *store.FileMeta) bool {
		return newCheckpointPaths.Contains(f.Path())
	})

	if len(newCheckpointFiles) != newCheckpointPaths.Len() {
		return nil, xerrors.New("failed in getting the file information")
	}

	// In the case where `deltasAfterCheckpoint` is empty, `deltas` should still not be empty,
	// they may just be before the checkpoint version unless we have a bug in log cleanup
	lastCommitTS := deltas[len(deltas)-1].TimeModified()

	return &LogSegment{
		LogPath:           r.logStore.Root(),
		Version:           newVersion,
		Deltas:            deltasAfterCheckpoint,
		Checkpoints:       newCheckpointFiles,
		CheckpointVersion: newCheckpointVersion,
		LastCommitTS:      lastCommitTS,
	}, nil
}

func (r *SnapshotReader) createSnapshot(segment *LogSegment, lastCommitTS int64) (*Snapshot, error) {
	minTS, err := r.minRetentionTS()
	if err != nil {
		return nil, xerrors.Errorf("unable to extract min retention: %w", err)
	}

	return NewSnapshot(r.logStore.Root(), segment.Version, segment, minTS, lastCommitTS, r.logStore, r.checkpointReader)
}

func (r *SnapshotReader) update() (*Snapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.updateInternal()
}

// updateInternal is not goroutine-safe, the caller should take care of locking.
func (r *SnapshotReader) updateInternal() (*Snapshot, error) {
	cur := r.currentSnapshot.Load()
	v := cur.segment.CheckpointVersion
	verSegment, err := r.logSegmentForVersion(v, -1)

	if err != nil && xerrors.Is(err, store.ErrFileNotFound) {
		if strings.Contains(err.Error(), "reconstruct state at version") {
			return nil, xerrors.Errorf("reconstruct err: %w", err)
		}

		logger.Log.Infof("No delta log found for the Delta table at " + r.logStore.Root())
		newSnapshot, err := NewInitialSnapshot(r.logStore.Root(), r.logStore, r.checkpointReader)
		if err != nil {
			return nil, xerrors.Errorf("unable to build initial snapshot: %w", err)
		}
		r.currentSnapshot.Store(newSnapshot)
		return newSnapshot, nil
	}

	if !cur.segment.equal(verSegment) {
		newSnapshot, err := r.createSnapshot(verSegment, verSegment.LastCommitTS.UnixMilli())
		if err != nil {
			return nil, xerrors.Errorf("unable to create snapshot: %w", err)
		}

		r.currentSnapshot.Store(newSnapshot)
		return newSnapshot, nil
	}

	return cur, nil
}

func verifyVersions(versions []int64) error {
	if len(versions) == 0 {
		return nil
	}
	for i := versions[0]; i <= versions[len(versions)-1]; i++ {
		if i != versions[i] {
			return xerrors.Errorf("version not continuous: %v", versions)
		}
	}
	return nil
}
