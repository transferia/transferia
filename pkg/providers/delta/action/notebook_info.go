//go:build !disable_delta_provider

package action

type NotebookInfo struct {
	NotebookID string `json:"notebookId,omitempty"`
}
