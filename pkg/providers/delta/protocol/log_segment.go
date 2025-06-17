//go:build !disable_delta_provider

package protocol

import (
	"time"

	"github.com/transferia/transferia/pkg/providers/delta/store"
)

type LogSegment struct {
	LogPath           string
	Version           int64
	Deltas            []*store.FileMeta
	Checkpoints       []*store.FileMeta
	CheckpointVersion int64
	LastCommitTS      time.Time
}

func (l *LogSegment) equal(other *LogSegment) bool {
	if other == nil {
		return false
	}
	if l.LogPath != other.LogPath ||
		l.Version != other.Version ||
		l.LastCommitTS.Unix() != other.LastCommitTS.Unix() {
		return false
	}
	if l.CheckpointVersion != other.CheckpointVersion {
		return false
	}
	return true
}

func newEmptyLogStatement(logPath string) *LogSegment {
	return &LogSegment{
		LogPath:           logPath,
		Version:           -1,
		Deltas:            nil,
		Checkpoints:       nil,
		CheckpointVersion: -1,
		LastCommitTS:      time.Time{},
	}
}
