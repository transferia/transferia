package util

import (
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func MapFromJSON(from interface{}, to interface{}) error {
	s, err := json.Marshal(from)
	if err != nil {
		return xerrors.Errorf("marshal: %w", err)
	}
	if err := json.Unmarshal(s, &to); err != nil {
		return xerrors.Errorf("unmarshal: %w", err)
	}
	return nil
}

func MapProtoJSON(from proto.Message, to proto.Message) error {
	s, err := protojson.Marshal(from)
	if err != nil {
		return xerrors.Errorf("marshal: %w", err)
	}
	if err := protojson.Unmarshal(s, to); err != nil {
		return xerrors.Errorf("unmarshal: %w", err)
	}
	return nil
}
