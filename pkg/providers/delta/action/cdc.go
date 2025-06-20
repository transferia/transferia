//go:build !disable_delta_provider

package action

import (
	"net/url"
)

type AddCDCFile struct {
	Path            string            `json:"path,omitempty"`
	DataChange      bool              `json:"dataChange,omitempty"`
	PartitionValues map[string]string `json:"partitionValues,omitempty"`
	Size            int64             `json:"size,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
}

func (a *AddCDCFile) IsDataChanged() bool {
	return a.DataChange
}

func (a *AddCDCFile) PathAsURI() (*url.URL, error) {
	return url.Parse(a.Path)
}

func (a *AddCDCFile) Wrap() *Single {
	res := new(Single)
	res.Cdc = a
	return res
}

func (a *AddCDCFile) JSON() (string, error) {
	return jsonString(a)
}
