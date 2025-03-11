package sample

import (
	"time"

	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
)

const (
	iotSampleType = "iot"
)

var (
	_ model.Source = (*SampleSource)(nil)
)

type SampleSource struct {
	SampleType         string
	TableName          string
	MaxSampleData      int64
	MinSleepTime       time.Duration
	SnapshotEventCount int64
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
