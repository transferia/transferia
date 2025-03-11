package coordinator

import "github.com/transferria/transferria/pkg/errors/coded"

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
