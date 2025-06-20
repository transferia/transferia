//go:build !disable_delta_provider

package protocol

import (
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/delta/action"
	store2 "github.com/transferia/transferia/pkg/providers/delta/store"
	"github.com/transferia/transferia/pkg/util/iter"
)

// Snapshot provides APIs to access the Delta table state (such as table metadata, active files) at some version.
// See Delta Transaction Log Protocol  for more details about the transaction logs.
type Snapshot struct {
	path             string
	version          int64
	segment          *LogSegment
	minTS            int64
	commitTS         int64
	store            store2.Store
	checkpointReader CheckpointReader

	state       *snapshotState
	activeFiles []*action.AddFile
	protocol    *action.Protocol
	metadata    *action.Metadata
	replayer    *MemoryOptimizedLogReplay
}

func NewSnapshot(
	path string,
	version int64,
	logsegment *LogSegment,
	minTS int64,
	commitTS int64,
	store store2.Store,
	checkpointReader CheckpointReader,
) (*Snapshot, error) {
	s := &Snapshot{
		path:             path,
		version:          version,
		segment:          logsegment,
		minTS:            minTS,
		commitTS:         commitTS,
		store:            store,
		checkpointReader: checkpointReader,
		state:            nil,
		activeFiles:      nil,
		protocol:         nil,
		metadata:         nil,
		replayer:         nil,
	}

	var err error
	s.state, err = s.loadState()
	if err != nil {
		return nil, xerrors.Errorf("unable to load state: %w", err)
	}

	s.replayer = &MemoryOptimizedLogReplay{
		files:            s.files(),
		logStore:         s.store,
		checkpointReader: s.checkpointReader,
	}
	s.activeFiles, err = s.loadActiveFiles()
	if err != nil {
		return nil, xerrors.Errorf("unable to load active files: %w", err)
	}
	s.protocol, s.metadata, err = s.loadTableProtoclAndMetadata()
	if err != nil {
		return nil, xerrors.Errorf("unable to load meta and protocol: %w", err)
	}
	return s, nil
}

func NewInitialSnapshot(path string, store store2.Store, cpReader CheckpointReader) (*Snapshot, error) {
	s := &Snapshot{
		path:             path,
		version:          -1,
		segment:          newEmptyLogStatement(path),
		minTS:            -1,
		commitTS:         -1,
		store:            store,
		checkpointReader: cpReader,
		state:            nil,
		activeFiles:      nil,
		protocol:         nil,
		metadata:         nil,
		replayer:         nil,
	}

	var err error
	s.state = new(snapshotState)
	s.activeFiles, err = s.loadActiveFiles()
	if err != nil {
		return nil, xerrors.Errorf("unable to load active files: %w", err)
	}

	s.protocol = action.DefaultProtocol()
	s.metadata = action.DefaultMetadata()

	return s, nil
}

func (s *Snapshot) loadTableProtoclAndMetadata() (*action.Protocol, *action.Metadata, error) {
	var protocol *action.Protocol = nil
	var metadata *action.Metadata = nil
	iter := s.replayer.GetReverseIterator()
	defer iter.Close()

	for iter.Next() {
		rt, err := iter.Value()
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to extract value: %w", err)
		}
		a := rt.act
		switch v := a.(type) {
		case *action.Protocol:
			if protocol == nil {
				protocol = v
				if protocol != nil && metadata != nil {
					return protocol, metadata, nil
				}
			}
		case *action.Metadata:
			if metadata == nil {
				metadata = v
				if metadata != nil && protocol != nil {
					return protocol, metadata, nil
				}
			}
		}
	}

	if protocol == nil {
		return nil, nil, xerrors.Errorf("unable to found protocol: %v", s.segment.Version)
	}
	if metadata == nil {
		return nil, nil, xerrors.Errorf("unable to found metadata: %v", s.segment.Version)
	}
	return nil, nil, xerrors.New("wtf, should never happens")
}

func (s *Snapshot) AllFiles() ([]*action.AddFile, error) {
	return s.activeFiles, nil
}

func (s *Snapshot) Metadata() (*action.Metadata, error) {
	return s.metadata, nil
}

// Version returns the version of this Snapshot
func (s *Snapshot) Version() int64 {
	return s.version
}

// CommitTS returns the time of commit for this Snapshot
func (s *Snapshot) CommitTS() time.Time {
	return time.Unix(0, s.commitTS*int64(time.Millisecond)).UTC()
}

func (s *Snapshot) tombstones() ([]*action.RemoveFile, error) {
	return iter.ToSlice(s.state.tombstones)
}

func (s *Snapshot) setTransactions() []*action.SetTransaction {
	return s.state.setTransactions
}

func (s *Snapshot) transactions() map[string]int64 {
	// appID to version
	trxs := s.setTransactions()
	res := make(map[string]int64, len(trxs))
	for _, trx := range trxs {
		res[trx.AppID] = int64(trx.Version)
	}
	return res
}

func (s *Snapshot) numOfFiles() (int64, error) {
	return s.state.numOfFiles, nil
}

func (s *Snapshot) files() []string {
	var res []string
	for _, f := range s.segment.Deltas {
		res = append(res, f.Path())
	}
	for _, f := range s.segment.Checkpoints {
		res = append(res, f.Path())
	}
	// todo: assert
	return res
}

func (s *Snapshot) loadInMemory(files []string) ([]*action.Single, error) {
	sort.Slice(files, func(i, j int) bool {
		return files[i] < files[j]
	})

	var actions []*action.Single
	for _, f := range files {
		if strings.HasSuffix(f, "json") {
			iter, err := s.store.Read(f)
			if err != nil {
				return nil, xerrors.Errorf("unable to read: %s: %w", f, err)
			}

			for iter.Next() {
				line, err := iter.Value()
				if err != nil {
					return nil, xerrors.Errorf("unable to iterate value: %w", err)
				}
				v := new(action.Single)
				if err := json.Unmarshal([]byte(line), &v); err != nil {
					return nil, xerrors.Errorf("unable to unmarshal: %w", err)
				}
				actions = append(actions, v)
			}
			_ = iter.Close()
		} else if strings.HasSuffix(f, "parquet") {
			iter, err := s.checkpointReader.Read(f)
			if err != nil {
				return nil, xerrors.Errorf("unable to read checkpoint: %s: %w", f, err)
			}
			for iter.Next() {
				s, err := iter.Value()
				if err != nil {
					return nil, xerrors.Errorf("unable to iterate value: %w", err)
				}

				actions = append(actions, s.Wrap())
			}
			_ = iter.Close()
		}
	}
	return actions, nil
}

type snapshotState struct {
	setTransactions      []*action.SetTransaction
	activeFiles          iter.Iter[*action.AddFile]
	tombstones           iter.Iter[*action.RemoveFile]
	sizeInBytes          int64
	numOfFiles           int64
	numOfRemoves         int64
	numOfSetTransactions int64
}

func (s *Snapshot) loadState() (*snapshotState, error) {
	replay := NewReplayer(s.minTS)
	singleActions, err := s.loadInMemory(s.files())
	if err != nil {
		return nil, err
	}

	actions := make([]action.Container, len(singleActions))
	for i, sa := range singleActions {
		actions[i] = sa.Unwrap()
	}
	if err := replay.Append(0, iter.FromSlice(actions...)); err != nil {
		return nil, xerrors.Errorf("unable to join actions: %w", err)
	}

	if replay.currentProtocol == nil {
		return nil, xerrors.Errorf("action protocol not found: %v", s.version)
	}
	if replay.currentMeta == nil {
		return nil, xerrors.Errorf("action metadata not found: %v", s.version)
	}

	return &snapshotState{
		setTransactions:      replay.GetSetTransactions(),
		activeFiles:          replay.GetActiveFiles(),
		tombstones:           replay.GetTombstones(),
		sizeInBytes:          replay.sizeInBytes,
		numOfFiles:           int64(len(replay.activeFiles)),
		numOfRemoves:         int64(len(replay.tombstones)),
		numOfSetTransactions: int64(len(replay.transactions)),
	}, nil
}

func (s *Snapshot) loadActiveFiles() ([]*action.AddFile, error) {
	return iter.ToSlice(s.state.activeFiles)
}
