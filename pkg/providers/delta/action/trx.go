//go:build !disable_delta_provider

package action

type SetTransaction struct {
	AppID       string `json:"appId,omitempty"`
	Version     int64  `json:"version,omitempty"`
	LastUpdated *int64 `json:"lastUpdated,omitempty"`
}

func (s *SetTransaction) Wrap() *Single {
	res := new(Single)
	res.Txn = s
	return res
}

func (s *SetTransaction) JSON() (string, error) {
	return jsonString(s)
}
