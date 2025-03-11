package stdout

import (
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type sinker struct {
	logger      log.Logger
	config      *StdoutDestination
	pushedTotal uint64
}

type window struct {
	left  time.Time
	right time.Time
}

func (s *sinker) Push(input []abstract.ChangeItem) error {
	lag := map[string]*window{}
	for _, i := range input {
		if lag[i.Table] == nil {
			lag[i.Table] = new(window)
		}

		cTime := time.Unix(0, int64(i.CommitTime))
		if lag[i.Table].left.Sub(cTime) > 0 {
			lag[i.Table].left = cTime
		}

		if lag[i.Table].right.Sub(cTime) < 0 {
			lag[i.Table].right = cTime
		}
	}
	if s.config.ShowData {
		if len(input) > 10 {
			s.logger.Infof("input: %v rows, sniff items:\n%s", len(input), abstract.Sniff(input))
		} else {
			abstract.Dump(input)
		}
	}
	s.pushedTotal += uint64(len(input))
	s.logger.Info("Pushed", log.Any("count", len(input)))
	return nil
}

func (s sinker) Close() error {
	s.logger.Info("Close sinker", log.UInt64("totalPushed", s.pushedTotal))
	return nil
}

func NewSinker(lgr log.Logger, config *StdoutDestination, registry metrics.Registry) abstract.Sinker {
	return &sinker{
		logger:      lgr,
		config:      config,
		pushedTotal: 0,
	}
}
