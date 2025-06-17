//go:build !disable_delta_provider

package protocol

import (
	"math"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/delta/action"
	"github.com/transferia/transferia/pkg/providers/delta/store"
	util_math "github.com/transferia/transferia/pkg/util/math"
)

type history struct {
	logStore store.Store
}

func (h *history) commitInfo(version int64) (*action.CommitInfo, error) {
	iter, err := h.logStore.Read(DeltaFile(h.logStore.Root(), version))
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var c *action.CommitInfo
	for iter.Next() {
		line, err := iter.Value()
		if err != nil {
			return nil, xerrors.Errorf("unable to read value: %w", err)
		}

		v, err := action.New(line)
		if err != nil {
			return nil, xerrors.Errorf("unable to construct action: %w", err)
		}

		if vv := v.Wrap(); vv != nil && vv.CommitInfo != nil {
			c = vv.CommitInfo
			break
		}
	}

	if c == nil {
		res := new(action.CommitInfo)
		res.Version = &version
		return res, nil
	} else {
		return c.Copy(version), nil
	}
}

func (h *history) checkVersionExists(versionToCkeck int64, sr *SnapshotReader) error {
	earliestVersion, err := h.getEarliestReproducibleCommitVersion()
	if err != nil {
		return xerrors.Errorf("unable to get earliest ver: %w", err)
	}

	s, err := sr.update()
	if err != nil {
		return xerrors.Errorf("unable to update snapshot reader: %w", err)
	}
	latestVersion := s.Version()
	if versionToCkeck < earliestVersion || versionToCkeck > latestVersion {
		return xerrors.Errorf("Cannot time travel Delta table to version %d, Available versions [%d, %d]", versionToCkeck, earliestVersion, latestVersion)
	}

	return nil
}

func (h *history) activeCommitAtTime(sr *SnapshotReader, timestamp int64,
	canReturnLastCommit bool, mustBeRecreatable bool, canReturnEarliestCommit bool,
) (*commit, error) {
	timeInMill := timestamp
	var earliestVersion int64
	var err error
	if mustBeRecreatable {
		earliestVersion, err = h.getEarliestReproducibleCommitVersion()
		if err != nil {
			return nil, xerrors.Errorf("unable to get earliest commit: %w", err)
		}
	} else {
		earliestVersion, err = h.getEarliestDeltaFile()
		if err != nil {
			return nil, xerrors.Errorf("unable to get earliest delta file: %w", err)
		}
	}

	s, err := sr.update()
	if err != nil {
		return nil, xerrors.Errorf("unable to update snapshot reader: %w", err)
	}
	latestVersion := s.Version()

	commits, err := h.getCommits(h.logStore, h.logStore.Root(), earliestVersion, latestVersion+1)
	if err != nil {
		return nil, xerrors.Errorf("unable to get commits: %w", err)
	}

	res := h.getLastCommitBeforeTimestamp(commits, timeInMill)
	if res == nil {
		res = commits[0]
	}

	commitTS := res.timestamp
	if res.timestamp > timeInMill && !canReturnEarliestCommit {
		return nil, xerrors.Errorf("The provided timestamp %d is before the earliest version available to this table (%v). Please use a timestamp greater than or equal to %d.", timeInMill, s.path, commitTS)
	} else if res.timestamp < timeInMill && res.version == latestVersion && !canReturnLastCommit {
		return nil, xerrors.Errorf("The provided timestamp %d is before the latest version available to this table (%v). Please use a timestamp less than or equal to %d.", timeInMill, s.path, commitTS)
	}

	return res, nil
}

func (h *history) getEarliestDeltaFile() (int64, error) {
	version0 := DeltaFile(h.logStore.Root(), 0)
	iter, err := h.logStore.ListFrom(version0)
	if err != nil {
		return 0, xerrors.Errorf("unable to list from: %w", err)
	}
	defer iter.Close()

	var earliestVersionOpt *store.FileMeta
	for iter.Next() {
		v, err := iter.Value()
		if err != nil {
			return 0, xerrors.Errorf("unable to get value: %w", err)
		}
		if IsDeltaFile(v.Path()) {
			earliestVersionOpt = v
			break
		}
	}
	if earliestVersionOpt == nil {
		return 0, xerrors.Errorf("no files found in the log dir: %s", h.logStore.Root())
	}
	return LogVersion(earliestVersionOpt.Path())
}

func (h *history) getEarliestReproducibleCommitVersion() (int64, error) {
	iter, err := h.logStore.ListFrom(DeltaFile(h.logStore.Root(), 0))
	if err != nil {
		return 0, xerrors.Errorf("unable to list store for commits: %w", err)
	}
	defer iter.Close()

	var files []*store.FileMeta
	for iter.Next() {
		f, err := iter.Value()
		if err != nil {
			return 0, xerrors.Errorf("unable to read file meta line: %w", err)
		}
		if IsCheckpointFile(f.Path()) || IsDeltaFile(f.Path()) {
			files = append(files, f)
		}
	}
	type checkpointStep struct {
		version  int64
		numParts int
	}

	checkpointMap := make(map[checkpointStep]int)
	smallestDeltaVersion := int64(math.MaxInt64)
	lastCompleteCheckpoint := int64(-1)

	for _, f := range files {

		nextFilePath := f.Path()
		if IsDeltaFile(nextFilePath) {
			version, err := LogVersion(nextFilePath)
			if version == 0 || err != nil {
				return 0, nil
			}
			smallestDeltaVersion = util_math.MinT(version, smallestDeltaVersion)
			if lastCompleteCheckpoint > 0 && lastCompleteCheckpoint >= smallestDeltaVersion {
				return lastCompleteCheckpoint, nil
			}
		} else if IsCheckpointFile(nextFilePath) {
			checkpointVersion, err := CheckpointVersion(nextFilePath)
			if err != nil {
				continue
			}
			parts, err := NumCheckpointParts(nextFilePath)
			if parts <= 0 || err != nil {
				lastCompleteCheckpoint = checkpointVersion
			} else {
				numParts := parts
				key := checkpointStep{version: checkpointVersion, numParts: numParts}
				preCount := checkpointMap[key]
				if numParts == preCount+1 {
					lastCompleteCheckpoint = checkpointVersion
				}
				checkpointMap[key] = preCount + 1
			}
		}
	}

	if lastCompleteCheckpoint > 0 && lastCompleteCheckpoint >= smallestDeltaVersion {
		return lastCompleteCheckpoint, nil
	} else if smallestDeltaVersion < math.MaxInt64 {
		return 0, xerrors.Errorf("no reproducible commit found in: %s", h.logStore.Root())
	} else {
		return 0, xerrors.Errorf("no files found in the log dir: %s", h.logStore.Root())
	}
}

func (h *history) getLastCommitBeforeTimestamp(commits []*commit, timeInMill int64) *commit {
	var i int
	for i = len(commits) - 1; i >= 0; i-- {
		if commits[i].timestamp <= timeInMill {
			break
		}
	}

	if i < 0 {
		return nil
	}
	return commits[i]
}

func (h *history) getCommits(logStore store.Store, logPath string, start int64, end int64) ([]*commit, error) {
	iter, err := logStore.ListFrom(DeltaFile(logPath, start))
	if err != nil {
		return nil, xerrors.Errorf("unable to list logs: %w", err)
	}
	defer iter.Close()

	var commits []*commit
	for iter.Next() {
		f, err := iter.Value()
		if err != nil {
			return nil, xerrors.Errorf("unable to get value: %w", err)
		}
		if IsDeltaFile(f.Path()) {
			ver, err := LogVersion(f.Path())
			if err != nil {
				continue
			}
			c := &commit{version: ver, timestamp: f.TimeModified().UnixMilli()}
			if c.version < end {
				commits = append(commits, c)
			} else {
				break
			}
		}
	}

	return commits, nil
}

type commit struct {
	version   int64
	timestamp int64
}

func (c *commit) Timestamp() int64 {
	return c.timestamp
}

func (c *commit) WithTimestamp(timestamp int64) *commit {
	return &commit{
		version:   c.version,
		timestamp: timestamp,
	}
}

func (c *commit) Version() int64 {
	return c.version
}
