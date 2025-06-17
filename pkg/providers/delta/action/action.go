//go:build !disable_delta_provider

package action

import (
	"encoding/json"
	"net/url"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type Container interface {
	Wrap() *Single
	JSON() (string, error)
}

type FileAction interface {
	Container
	PathAsURI() (*url.URL, error)
	IsDataChanged() bool
}

func New(raw string) (Container, error) {
	action := new(Single)
	if err := json.Unmarshal([]byte(raw), action); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal action: %w", err)
	}

	return action.Unwrap(), nil
}

func jsonString(a Container) (string, error) {
	b, err := json.Marshal(a.Wrap())
	if err != nil {
		return "", xerrors.Errorf("unable to unmarshal action: %w", err)
	}
	return string(b), nil
}

type Single struct {
	Txn        *SetTransaction `json:"txn,omitempty"`
	Add        *AddFile        `json:"add,omitempty"`
	Remove     *RemoveFile     `json:"remove,omitempty"`
	MetaData   *Metadata       `json:"metaData,omitempty"`
	Protocol   *Protocol       `json:"protocol,omitempty"`
	Cdc        *AddCDCFile     `json:"cdc,omitempty"`
	CommitInfo *CommitInfo     `json:"commitInfo,omitempty"`
}

func (s *Single) Unwrap() Container {
	if s.Add != nil {
		return s.Add
	} else if s.Remove != nil {
		return s.Remove
	} else if s.MetaData != nil {
		return s.MetaData
	} else if s.Txn != nil {
		return s.Txn
	} else if s.Protocol != nil {
		return s.Protocol
	} else if s.Cdc != nil {
		return s.Cdc
	} else if s.CommitInfo != nil {
		return s.CommitInfo
	} else {
		return nil
	}
}
