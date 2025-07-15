package pusher

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type PusherState struct {
	mu            sync.Mutex
	logger        log.Logger
	inflightLimit int64
	inflightBytes int64
	PushProgress  map[string]Progress
	counter       int
}

func (s *PusherState) IsEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.counter == 0
}

type Progress struct {
	ReadOffsets []any
	Done        bool
}

// setPushProgress stores some useful information for tracking the read progress.
// For each file a Progress struct is kept in memory indicating which offsets where already read.
// Additionally a Done holds information if a file is fully read.
// The counterpart to setPushProgress is the ackPushProgress where the processed chunks are removed form state.
func (s *PusherState) setPushProgress(filePath string, offset any, isLast bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counter++

	progress, ok := s.PushProgress[filePath]
	if ok {
		progress.ReadOffsets = append(progress.ReadOffsets, offset)
		progress.Done = isLast
		s.PushProgress[filePath] = progress
	} else {
		// new file processing
		s.PushProgress[filePath] = Progress{
			ReadOffsets: []any{offset},
			Done:        isLast,
		}
	}
}

// ackPushProgress removes already processed chunks form state.
// It returns an error if chunk is double ack or missing.
func (s *PusherState) ackPushProgress(filePath string, offset any, isLast bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counter--

	progress, ok := s.PushProgress[filePath]
	if ok {
		newState := s.removeOffset(offset, progress.ReadOffsets)
		if len(newState) == len(progress.ReadOffsets) {
			// something wrong nothing in state but ack called on it
			return false, xerrors.Errorf("failed to ack: file %s at offset %v has no stored state", filePath, offset)
		}

		progress.Done = isLast
		progress.ReadOffsets = newState
		s.PushProgress[filePath] = progress

		if len(newState) == 0 && isLast {
			// done
			s.deleteDone(filePath)
			return true, nil
		}

		return false, nil
	} else {
		// should never reach here, ack something that was not pushed
		return false, xerrors.Errorf("failed to ack: file %s at offset %v has no stored state", filePath, offset)
	}
}

func (s *PusherState) removeOffset(toRemove any, offsets []any) []any {
	var remaining []any
	for _, offset := range offsets {
		if offset == toRemove {
			continue
		}
		remaining = append(remaining, offset)
	}

	return remaining
}

// DeleteDone delete's a processed files form state if the read process is completed
func (s *PusherState) deleteDone(filePath string) {
	// to be called on commit of state to, to keep map as small as possible
	progress, ok := s.PushProgress[filePath]
	if ok && progress.Done {
		delete(s.PushProgress, filePath)
	}
}

func (s *PusherState) waitLimits(ctx context.Context) {
	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.Reset()
	backoffTimer.MaxElapsedTime = 0
	nextLogDuration := backoffTimer.NextBackOff()
	logTime := time.Now()

	for !s.inLimits() {
		time.Sleep(time.Millisecond * 10)
		if ctx.Err() != nil {
			s.logger.Warn("context aborted, stop wait for limits")
			return
		}
		if time.Since(logTime) > nextLogDuration {
			logTime = time.Now()
			nextLogDuration = backoffTimer.NextBackOff()
			s.logger.Warnf(
				"reader throttled for %v, limits: %v bytes / %v bytes",
				backoffTimer.GetElapsedTime(),
				s.inflightBytes,
				s.inflightLimit,
			)
		}
	}
}

func (s *PusherState) inLimits() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflightLimit == 0 || s.inflightLimit > s.inflightBytes
}

func (s *PusherState) addInflight(size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflightBytes += size
}

func (s *PusherState) reduceInflight(size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflightBytes = s.inflightBytes - size
}
