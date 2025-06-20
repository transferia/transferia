//go:build !disable_delta_provider

package protocol

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/delta/action"
	"github.com/transferia/transferia/pkg/providers/delta/store"
	"github.com/transferia/transferia/pkg/util/iter"
)

type Replayer struct {
	MinTS int64

	currentProtocol *action.Protocol
	currentVer      int64
	currentMeta     *action.Metadata
	sizeInBytes     int64
	numMeta         int64
	numProtocol     int64
	transactions    map[string]*action.SetTransaction
	activeFiles     map[string]*action.AddFile
	tombstones      map[string]*action.RemoveFile
}

func NewReplayer(minTS int64) *Replayer {
	return &Replayer{
		MinTS:           minTS,
		currentProtocol: nil,
		currentVer:      0,
		currentMeta:     nil,
		sizeInBytes:     0,
		numMeta:         0,
		numProtocol:     0,
		transactions:    make(map[string]*action.SetTransaction),
		activeFiles:     make(map[string]*action.AddFile),
		tombstones:      make(map[string]*action.RemoveFile),
	}
}

func (r *Replayer) GetSetTransactions() []*action.SetTransaction {
	values := make([]*action.SetTransaction, 0, len(r.transactions))
	for _, v := range r.transactions {
		values = append(values, v)
	}
	return values
}

func (r *Replayer) GetActiveFiles() iter.Iter[*action.AddFile] {
	values := make([]*action.AddFile, 0, len(r.activeFiles))
	for _, v := range r.activeFiles {
		values = append(values, v)
	}
	return iter.FromSlice(values...)
}

func (r *Replayer) GetTombstones() iter.Iter[*action.RemoveFile] {
	values := make([]*action.RemoveFile, 0, len(r.tombstones))
	for _, v := range r.tombstones {
		if v.DelTimestamp() > r.MinTS {
			values = append(values, v)
		}
	}
	return iter.FromSlice(values...)
}

func (r *Replayer) Append(version int64, iter iter.Iter[action.Container]) error {
	if r.currentVer == -1 || version == r.currentVer+1 {
		return xerrors.Errorf("attempted to replay version %d, but state is at %d", version, r.currentVer)
	}
	r.currentVer = version

	for {
		ok := iter.Next()
		if !ok {
			break
		}

		act, err := iter.Value()
		if err != nil {
			return xerrors.Errorf("unable to read value: %w", err)
		}

		switch v := act.(type) {
		case *action.SetTransaction:
			r.transactions[v.AppID] = v
		case *action.Metadata:
			r.currentMeta = v
			r.numMeta += 1
		case *action.Protocol:
			r.currentProtocol = v
			r.numProtocol += 1
		case *action.AddFile:

			canonicalPath := v.Path
			canonicalizedAdd := v.Copy(false, canonicalPath)

			r.activeFiles[canonicalPath] = canonicalizedAdd
			delete(r.tombstones, canonicalPath)
			r.sizeInBytes += canonicalizedAdd.Size
		case *action.RemoveFile:
			canonicalPath := v.Path
			canonicalizedRemove := v.Copy(false, canonicalPath)

			if removeFile, ok := r.activeFiles[canonicalPath]; ok {
				delete(r.activeFiles, canonicalPath)
				r.sizeInBytes -= removeFile.Size
			}
			r.tombstones[canonicalPath] = canonicalizedRemove
		default:
			// do nothing
		}
	}

	return iter.Close()
}

type replayTuple struct {
	act            action.Container
	fromCheckpoint bool
}

type MemoryOptimizedLogReplay struct {
	files    []string
	logStore store.Store
	// timezone      time.Location
	checkpointReader CheckpointReader
}

func (m *MemoryOptimizedLogReplay) GetReverseIterator() iter.Iter[*replayTuple] {
	sort.Slice(m.files, func(i, j int) bool {
		return m.files[i] > m.files[j]
	})
	reverseFilesIter := iter.FromSlice(m.files...)

	return &logReplayIterator{
		logStore:         m.logStore,
		checkpointReader: m.checkpointReader,
		reverseFilesIter: reverseFilesIter,
		actionIter:       nil,
	}
}

var _ iter.Iter[*replayTuple] = new(customJSONIterator)

type customJSONIterator struct {
	iter iter.Iter[string]
}

func (r *customJSONIterator) Next() bool {
	return r.iter.Next()
}

func (r *customJSONIterator) Value() (*replayTuple, error) {
	str, err := r.iter.Value()
	if err != nil {
		return nil, xerrors.Errorf("unable to read value: %w", err)
	}

	act := new(action.Single)
	err = json.Unmarshal([]byte(str), &act)
	if err != nil {
		return nil, xerrors.Errorf("unable to unmarshal: %w", err)
	}

	return &replayTuple{
		act:            act.Unwrap(),
		fromCheckpoint: false,
	}, nil
}

func (r *customJSONIterator) Close() error {
	return r.iter.Close()
}

type customParquetIterator struct {
	iter iter.Iter[action.Container]
}

func (c *customParquetIterator) Close() error {
	return c.iter.Close()
}

func (c *customParquetIterator) Next() bool {
	return c.iter.Next()
}

func (c *customParquetIterator) Value() (*replayTuple, error) {
	a, err := c.iter.Value()
	if err != nil {
		return nil, xerrors.Errorf("unable to read value: %w", err)
	}

	return &replayTuple{
		act:            a,
		fromCheckpoint: true,
	}, nil
}

type logReplayIterator struct {
	logStore         store.Store
	checkpointReader CheckpointReader
	reverseFilesIter iter.Iter[string]
	actionIter       iter.Iter[*replayTuple]
}

func (l *logReplayIterator) getNextIter() (iter.Iter[*replayTuple], error) {
	nextFile, err := l.reverseFilesIter.Value()
	if err != nil {
		return nil, xerrors.Errorf("unable to read reversed values: %w", err)
	}

	if strings.HasSuffix(nextFile, ".json") {
		lines, err := l.logStore.Read(nextFile)
		if err != nil {
			return nil, xerrors.Errorf("unable to read json checkpoint: %w", err)
		}
		return &customJSONIterator{iter: lines}, nil
	} else if strings.HasSuffix(nextFile, ".parquet") {
		lines, err := l.checkpointReader.Read(nextFile)
		if err != nil {
			return nil, xerrors.Errorf("unable to read parquet checkpoint: %w", err)
		}
		return &customParquetIterator{iter: lines}, nil
	} else {
		return nil, xerrors.Errorf("unexpected log file path: %s", nextFile)
	}
}

func (l *logReplayIterator) ensureNextIterReady() error {
	if l.actionIter != nil && l.actionIter.Next() {
		return nil
	}

	if l.actionIter != nil {
		if err := l.actionIter.Close(); err != nil {
			return xerrors.Errorf("unable to close action iter: %w", err)
		}
	}

	l.actionIter = nil

	for l.reverseFilesIter.Next() {
		fiter, err := l.getNextIter()
		if err != nil {
			return xerrors.Errorf("unable to get next iter: %w", err)
		}
		l.actionIter = fiter

		if l.actionIter.Next() {
			return nil
		}

		if err := l.actionIter.Close(); err != nil {
			return xerrors.Errorf("unable to close action iter: %w", err)
		}

		l.actionIter = nil
	}

	return nil
}

func (l *logReplayIterator) Next() bool {
	if err := l.ensureNextIterReady(); err != nil {
		return false
	}

	return l.actionIter != nil
}

func (l *logReplayIterator) Value() (*replayTuple, error) {
	if !l.Next() {
		return nil, xerrors.New("no element")
	}
	if l.actionIter == nil {
		return nil, xerrors.New("impossible")
	}
	return l.actionIter.Value()
}

func (l *logReplayIterator) Close() error {
	if l.actionIter != nil {
		return l.actionIter.Close()
	}
	return nil
}
