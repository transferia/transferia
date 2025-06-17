//go:build !disable_yt_provider

package tablemeta

import (
	"strings"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type YtTableMeta struct {
	Cluster    string
	Prefix     string
	Name       string
	RowCount   int64
	NodeID     *yt.NodeID
	DataWeight int64
}

func (t *YtTableMeta) FullName() string {
	return t.Cluster + "." + t.OriginalPath()
}

func (t *YtTableMeta) OriginalPath() string {
	return t.Prefix + "/" + t.Name
}

func (t *YtTableMeta) OriginalYPath() ypath.YPath {
	return ypath.NewRich(t.OriginalPath())
}

func NewYtTableMeta(cluster, prefix, name string, rows, weight int64) *YtTableMeta {
	return &YtTableMeta{
		Cluster:    cluster,
		Prefix:     prefix,
		Name:       strings.TrimPrefix(name, "/"),
		RowCount:   rows,
		DataWeight: weight,
		NodeID:     nil,
	}
}

type YtTables []*YtTableMeta
