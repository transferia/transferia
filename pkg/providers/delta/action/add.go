//go:build !disable_delta_provider

package action

import (
	"net/url"

	"github.com/transferia/transferia/pkg/util"
)

type AddFile struct {
	Path             string            `json:"path,omitempty"`
	DataChange       bool              `json:"dataChange,omitempty"`
	PartitionValues  map[string]string `json:"partitionValues,omitempty"`
	Size             int64             `json:"size,omitempty"`
	ModificationTime int64             `json:"modificationTime,omitempty"`
	Stats            string            `json:"stats,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
}

func (a *AddFile) IsDataChanged() bool {
	return a.DataChange
}

func (a *AddFile) PathAsURI() (*url.URL, error) {
	return url.Parse(a.Path)
}

func (a *AddFile) Wrap() *Single {
	res := new(Single)
	res.Add = a
	return res
}

func (a *AddFile) JSON() (string, error) {
	return jsonString(a)
}

func (a *AddFile) Copy(dataChange bool, path string) *AddFile {
	dst := new(AddFile)
	_ = util.MapFromJSON(a, dst)
	dst.Path = path
	dst.DataChange = dataChange
	return dst
}
