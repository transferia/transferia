package coordinator

import (
	"github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/errors/coded"
)

type StatusMessageType string

const (
	WarningStatusMessageType = StatusMessageType("TYPE_WARNING")
	ErrorStatusMessageType   = StatusMessageType("TYPE_ERROR")
)

type StatusMessage struct {
	Type       StatusMessageType `json:"type"`
	Heading    string            `json:"heading"`
	Message    string            `json:"message"`
	Categories []string          `json:"categories"`
	Code       coded.Code        `json:"code"`
	ID         string            `json:"id"`
}

// IsProlongableWith other checks that message can be prolonged with provided status message instead of reopening
func (s *StatusMessage) IsProlongableWith(other *StatusMessage) bool {
	categoriesEqual := slices.EqualUnordered(s.Categories, other.Categories)
	return s.Type == other.Type && s.Heading == other.Heading && s.Message == other.Message && categoriesEqual && s.Code == other.Code
}
