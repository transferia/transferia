//go:build !disable_delta_provider

package protocol

import (
	"encoding/json"
	"sort"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/delta/store"
	"github.com/transferia/transferia/pkg/util/math"
)

const LastCheckpointPath string = "_last_checkpoint"

var MaxInstance = CheckpointInstance{Version: -1, NumParts: 0}

type CheckpointMetaData struct {
	Version int64  `json:"version,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Parts   *int64 `json:"parts,omitempty"`
}

type CheckpointInstance struct {
	Version  int64
	NumParts int64
}

func (t *CheckpointInstance) Compare(other CheckpointInstance) int {
	if t.Version == other.Version {
		return int(t.NumParts - other.NumParts)
	}
	if t.Version-other.Version < 0 {
		return -1
	} else {
		return 1
	}
}

// IsEarlierThan compare based just on version and amount of parts in checkpoint
func (t *CheckpointInstance) IsEarlierThan(other CheckpointInstance) bool {
	return t.IsNotLaterThan(other) || (t.Version == other.Version && t.NumParts < other.NumParts)
}

// IsNotLaterThan compare based just on version
func (t *CheckpointInstance) IsNotLaterThan(other CheckpointInstance) bool {
	if other.Compare(MaxInstance) == 0 {
		return true
	}
	return t.Compare(other) <= 0
}

func (t *CheckpointInstance) GetCorrespondingFiles(dir string) (res []string) {
	if t.NumParts == 0 {
		return []string{CheckpointFileSingular(dir, t.Version)}
	} else {
		return CheckpointFileWithParts(dir, t.Version, int(t.NumParts))
	}
}

func FromPath(path string) (*CheckpointInstance, error) {
	version, err := CheckpointVersion(path)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse version: %s: %w", path, err)
	}
	numParts, err := NumCheckpointParts(path)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse num parts: %s: %w", path, err)
	}
	return &CheckpointInstance{Version: version, NumParts: int64(numParts)}, nil
}

func FromMetadata(metadata CheckpointMetaData) *CheckpointInstance {
	i := &CheckpointInstance{
		Version:  metadata.Version,
		NumParts: 0,
	}
	if metadata.Parts != nil {
		i.NumParts = *metadata.Parts
	}

	return i
}

func LastCheckpoint(s store.Store) (*CheckpointMetaData, error) {
	return LoadMetadataFromFile(s)
}

func LoadMetadataFromFile(s store.Store) (*CheckpointMetaData, error) {
	checkpoint, err := backoff.RetryWithData(func() (*CheckpointMetaData, error) {
		lines, err := s.Read(LastCheckpointPath)
		if err != nil {
			if xerrors.Is(err, store.ErrFileNotFound) {
				return nil, nil
			} else {
				return nil, xerrors.Errorf("unable to read last checkpoint: %w", err)
			}
		}

		if !lines.Next() {
			logger.Log.Warn("failed to read last checkpoint, end of iterator, try again")
			return nil, xerrors.New("no lines found")
		}

		line, err := lines.Value()
		if err != nil {
			logger.Log.Warn("failed to get line from iterator when reading last checkpoint, try again")
			_ = lines.Close()
			return nil, xerrors.Errorf("unable to get line: %w", err)
		}

		res := new(CheckpointMetaData)
		err = json.Unmarshal([]byte(line), res)
		if err != nil {
			logger.Log.Warn("failed to unmarshal json line when reading last checkpoint, try again")
			return nil, xerrors.Errorf("unable to unmarshal checkpoint: %w", err)
		}
		return res, nil
	}, backoff.NewExponentialBackOff())
	if err == nil {
		return checkpoint, nil
	}

	// tried N-times, still failed, can not find last_checkpoint
	// Hit a partial file. This could happen on Azure as overwriting _last_checkpoint file is
	// not atomic. We will try to list all files to find the latest checkpoint and restore
	// CheckpointMetaData from it.
	if lastCheckpoint, err := FindLastCompleteCheckpoint(s, MaxInstance); err != nil {
		return nil, xerrors.Errorf("unable to find last complete checkpoint: %w", err)
	} else {
		return metadataFromCheckpoint(lastCheckpoint), nil
	}
}

func metadataFromCheckpoint(checkpoint *CheckpointInstance) *CheckpointMetaData {
	if checkpoint == nil {
		return nil
	}

	if p := checkpoint.NumParts; p > 0 {
		return &CheckpointMetaData{Version: checkpoint.Version, Size: -1, Parts: &p}
	} else {
		return &CheckpointMetaData{Version: checkpoint.Version, Size: -1, Parts: nil}
	}
}

func FindLastCompleteCheckpoint(s store.Store, cv CheckpointInstance) (*CheckpointInstance, error) {
	cur := cv.Version
	for cur >= 0 {
		iter, err := s.ListFrom(CheckpointPrefix(s.Root(), math.MaxT(0, cur-1000)))
		if err != nil {
			return nil, xerrors.Errorf("unable to list checkpoints: %w", err)
		}

		var checkpoints []*CheckpointInstance
		for f, err := iter.Value(); iter.Next(); f, err = iter.Value() {
			if err != nil {
				return nil, xerrors.Errorf("unable to read checkpoint line: %w", err)
			}
			if !IsCheckpointFile(f.Path()) {
				continue
			}
			cp, err := FromPath(f.Path())
			if err != nil {
				continue
			}
			if cur == 0 || cp.Version <= cur || cp.IsEarlierThan(cv) {
				checkpoints = append(checkpoints, cp)
			} else {
				break
			}
		}

		lastCheckpoint := LatestCompleteCheckpoint(checkpoints, cv)
		if lastCheckpoint != nil {
			return lastCheckpoint, nil
		} else {
			cur -= 1000
		}
	}

	return nil, nil
}

type instanceKey struct {
	Version  int64
	NumParts int
	HasParts bool
}

func (i instanceKey) toInstance() *CheckpointInstance {
	if i.HasParts {
		return &CheckpointInstance{Version: i.Version, NumParts: int64(i.NumParts)}
	}
	return &CheckpointInstance{Version: i.Version, NumParts: 0}
}

func LatestCompleteCheckpoint(instances []*CheckpointInstance, notLaterThan CheckpointInstance) *CheckpointInstance {
	grouped := make(map[instanceKey][]*CheckpointInstance)
	for _, i := range instances {
		if !i.IsNotLaterThan(notLaterThan) {
			continue
		}
		k := instanceKey{Version: i.Version, NumParts: int(i.NumParts), HasParts: i.NumParts > 0}
		if vals, ok := grouped[k]; ok {
			vals = append(vals, i)
			grouped[k] = vals
		} else {
			grouped[k] = []*CheckpointInstance{i}
		}
	}

	var res []*CheckpointInstance
	for k, v := range grouped {
		if k.NumParts != len(v) {
			continue
		}
		res = append(res, k.toInstance())
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Compare(*res[j]) < 0
	})

	if len(res) != 0 {
		return res[len(res)-1]
	}
	return nil
}
