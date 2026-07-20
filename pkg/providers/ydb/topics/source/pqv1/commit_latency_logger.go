package pqv1source

import (
	"sync"
	"time"

	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	slowCommitAckThreshold = time.Minute
	maxTrackedCommits      = 10_000
)

type commitLatencyLogger struct {
	logger    log.Logger
	mu        sync.Mutex
	sessionID uint64 // sessionID prevents commits from an old reader session from being tracked after reset.
	startedAt map[persqueue.PartitionCookie]time.Time
}

// newCommitLatencyLogger creates a tracker that logs commit acknowledgements exceeding the latency threshold.
func newCommitLatencyLogger(logger log.Logger) *commitLatencyLogger {
	return &commitLatencyLogger{
		logger:    logger,
		mu:        sync.Mutex{},
		sessionID: 0,
		startedAt: make(map[persqueue.PartitionCookie]time.Time),
	}
}

func (l *commitLatencyLogger) wrap(data *persqueue.Data) func() {
	commitF := data.Commit
	cookie := data.PartitionCookie()

	l.mu.Lock()
	sessionID := l.sessionID
	l.mu.Unlock()

	return func() {
		l.mu.Lock()
		if sessionID == l.sessionID && len(l.startedAt) < maxTrackedCommits {
			l.startedAt[cookie] = time.Now()
		}
		l.mu.Unlock()
		commitF()
	}
}

func (l *commitLatencyLogger) handleAck(ack *persqueue.CommitAck) {
	acknowledgedAt := time.Now()
	durations := make([]string, len(ack.PartitionCookies))
	for i, cookie := range ack.PartitionCookies {
		l.mu.Lock()
		startedAt, ok := l.startedAt[cookie]
		delete(l.startedAt, cookie)
		l.mu.Unlock()

		if !ok {
			durations[i] = "not tracked"
			continue
		}
		duration := acknowledgedAt.Sub(startedAt)
		durations[i] = duration.String()
		if duration < slowCommitAckThreshold {
			continue
		}
		l.logger.Warn("Slow commit acknowledgement",
			log.UInt64("assign_id", cookie.AssignID),
			log.UInt64("partition_cookie", cookie.PartitionCookie),
			log.Int64("duration_ms", duration.Milliseconds()),
		)
	}
	l.logger.Infof("Ack: %v, durations: %v", ack.Cookies, durations)
}

func (l *commitLatencyLogger) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.sessionID++
	l.startedAt = make(map[persqueue.PartitionCookie]time.Time)
}
