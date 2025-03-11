package stats

import "github.com/transferia/transferia/library/go/core/metrics"

type NotificationStats struct {
	registry metrics.Registry

	// Sent is the number of notifications sent successfully
	Sent metrics.Counter
	// Errors is the number of errors
	Errors metrics.Counter
}

func NewNotificationStats(registry metrics.Registry) *NotificationStats {
	return &NotificationStats{
		registry: registry,

		Sent:   registry.Counter("notifications.sent"),
		Errors: registry.Counter("notifications.errors"),
	}
}
