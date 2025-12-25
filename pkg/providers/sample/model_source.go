package sample

import (
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.uber.org/zap/zapcore"
)

const (
	iotSampleType = "iot"
)

var (
	_ model.Source = (*SampleSource)(nil)
)

type SampleSource struct {
	SampleType         string        `log:"true"`
	TableName          string        `log:"true"`
	MaxSampleData      int64         `log:"true"`
	MinSleepTime       time.Duration `log:"true"`
	SnapshotEventCount int64         `log:"true"`
}

func (s *SampleSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(s, enc)
}

func (s *SampleSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *SampleSource) Validate() error {
	return nil
}

func (s *SampleSource) WithDefaults() {
	if s.SampleType == "" {
		s.SampleType = iotSampleType
	}
	if s.MaxSampleData == 0 {
		s.MaxSampleData = 10
	}
	if s.MinSleepTime == 0 {
		s.MinSleepTime = 1 * time.Second
	}
	if s.TableName == "" {
		s.TableName = s.SampleType
	}
	if s.SnapshotEventCount == 0 {
		s.SnapshotEventCount = 300_000
	}
}

func (s *SampleSource) IsSource() {}
