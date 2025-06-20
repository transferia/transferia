//go:build !disable_delta_provider

package action

type Format struct {
	Provider string            `json:"provider,omitempty"`
	Options  map[string]string `json:"options,omitempty"`
}
