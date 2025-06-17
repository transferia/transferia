//go:build !disable_delta_provider

package protocol

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/delta/action"
	store2 "github.com/transferia/transferia/pkg/providers/delta/store"
)

type TableLog struct {
	dataPath       string
	logPath        string
	store          store2.Store
	history        *history
	snapshotReader *SnapshotReader
}

// NewTableLog Create a DeltaLog instance representing the table located at the provided path.
func NewTableLog(dataPath string, logStore store2.Store) (*TableLog, error) {
	logPath := strings.TrimRight(dataPath, "/") + "/_delta_log/"

	reader, err := NewCheckpointReader(logStore)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct checkpoint reader: %s: %w", logPath, err)
	}

	hm := &history{logStore: logStore}
	sr, err := NewSnapshotReader(reader, logStore, hm)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct snapshot reader: %s: %w", logPath, err)
	}

	return &TableLog{
		dataPath:       dataPath,
		logPath:        logPath,
		store:          logStore,
		history:        hm,
		snapshotReader: sr,
	}, nil
}

// Snapshot the current Snapshot of the Delta table.
// You may need to call update() to access the latest Snapshot if the current Snapshot is stale.
func (l *TableLog) Snapshot() (*Snapshot, error) {
	return l.snapshotReader.currentSnapshot.Load(), nil
}

func (l *TableLog) Update() (*Snapshot, error) {
	return l.snapshotReader.update()
}

func (l *TableLog) SnapshotForVersion(version int64) (*Snapshot, error) {
	return l.snapshotReader.forVersion(version)
}

func (l *TableLog) SnapshotForTimestamp(timestamp int64) (*Snapshot, error) {
	return l.snapshotReader.forTimestamp(timestamp)
}

func (l *TableLog) CommitInfoAt(version int64) (*action.CommitInfo, error) {
	if err := l.history.checkVersionExists(version, l.snapshotReader); err != nil {
		return nil, xerrors.Errorf("unable to check version: %v exist: %w", version, err)
	}

	return l.history.commitInfo(version)
}

func (l *TableLog) Path() string {
	return l.dataPath
}

func (l *TableLog) TableExists() bool {
	return l.snapshotReader.snapshot().Version() >= 0
}
