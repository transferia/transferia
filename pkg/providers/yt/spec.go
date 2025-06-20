//go:build !disable_yt_provider

package yt

import (
	"encoding/json"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/jsonx"
)

type YTSpec struct {
	config map[string]interface{}
}

func NewYTSpec(config map[string]interface{}) *YTSpec {
	return &YTSpec{
		config: config,
	}
}

func (s *YTSpec) UnmarshalJSON(data []byte) error {
	return ParseYtSpec(string(data), &s.config)
}

func (s YTSpec) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.config)
}

func (s YTSpec) MarshalBinary() (data []byte, err error) {
	return s.MarshalJSON()
}

func (s *YTSpec) UnmarshalBinary(data []byte) error {
	return s.UnmarshalJSON(data)
}

func (s *YTSpec) GetConfig() map[string]interface{} {
	return s.config
}

func (s *YTSpec) IsEmpty() bool {
	return len(s.config) == 0
}

func ParseYtSpec(jsonStr string, spec *map[string]interface{}) error {
	if err := jsonx.NewDefaultDecoder(strings.NewReader(jsonStr)).Decode(spec); err != nil {
		return xerrors.Errorf("failed decode json: %w", err)
	}
	castNumbers(spec)
	return nil
}

func castNumbers(spec *map[string]interface{}) {
	for k := range *spec {
		switch t := (*spec)[k].(type) {
		case json.Number:
			(*spec)[k] = tryCastNumber(t)
		case map[string]interface{}:
			castNumbers(&t)
		default:
		}
	}
}

func tryCastNumber(v json.Number) interface{} {
	if l, err := v.Int64(); err != nil {
		if f, err := v.Float64(); err == nil {
			return f
		}
	} else {
		return l
	}
	return v
}
