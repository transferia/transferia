package model

import (
	"encoding/json"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"go.uber.org/zap/zapcore"
)

type MockDestination struct {
	SinkerFactory func() abstract.Sinker
	Cleanup       CleanupType
}

var _ Destination = (*MockDestination)(nil)

func (d *MockDestination) SafeToLog() {}

func (d *MockDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *MockDestination) MarshalJSON() ([]byte, error) { // custom JSON serializer - to be able to marshal MockDestination (default isn't working bcs of SinkerFactory)
	return json.Marshal(&struct {
		Cleanup CleanupType
	}{
		Cleanup: d.Cleanup,
	})
}

func (d *MockDestination) UnmarshalJSON(data []byte) error {
	aux := &struct {
		Cleanup CleanupType
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	d.Cleanup = aux.Cleanup
	return nil
}

func (d *MockDestination) IsDestination() {
}

func (d *MockDestination) WithDefaults() {
	if d.Cleanup == "" {
		d.Cleanup = DisabledCleanup
	}
}

func (d *MockDestination) CleanupMode() CleanupType {
	return d.Cleanup
}

func (d *MockDestination) Transformer() map[string]string {
	return nil
}

func (d *MockDestination) GetProviderType() abstract.ProviderType {
	return abstract.ProviderTypeMock
}

func (d *MockDestination) Validate() error {
	return nil
}
