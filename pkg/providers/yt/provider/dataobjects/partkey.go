//go:build !disable_yt_provider

package dataobjects

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

type PartKey interface {
	TableKey() string
	Range() ypath.Range
	String() (string, error)
}

type partKey struct {
	NodeID yt.NodeID   `yson:"id"`
	Table  string      `yson:"t"`
	Rng    ypath.Range `yson:"r"`
}

func (k *partKey) TableKey() string {
	return k.NodeID.String()
}

func (k *partKey) Range() ypath.Range {
	return k.Rng
}

func (k *partKey) String() (string, error) {
	b, err := yson.MarshalFormat(k, yson.FormatText)
	return string(b), err
}

func ParsePartKey(k string) (partKey, error) {
	var p partKey
	if err := yson.Unmarshal([]byte(k), &p); err != nil {
		return p, xerrors.Errorf("unable to unmarshal part key %s: %w", k, err)
	}
	return p, nil
}
