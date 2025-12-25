package stdout

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.uber.org/zap/zapcore"
)

type EmptySource struct{}

var _ model.Source = (*EmptySource)(nil)

func (s *EmptySource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(s, enc)
}

func (EmptySource) WithDefaults() {
}

func (s *EmptySource) Include(tID abstract.TableID) bool {
	return len(s.FulfilledIncludes(tID)) > 0
}

func (*EmptySource) FulfilledIncludes(tID abstract.TableID) []string {
	return []string{""}
}

func (*EmptySource) AllIncludes() []string {
	return nil
}

func (EmptySource) IsSource() {
}

func (s *EmptySource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *EmptySource) Validate() error {
	return nil
}
