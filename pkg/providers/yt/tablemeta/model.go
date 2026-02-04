package tablemeta

import (
	"strings"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type YtTableMeta struct {
	Cluster         string
	Prefix          string
	Name            string
	RowCount        int64
	Columns         []string
	NodeID          *yt.NodeID
	DataWeight      int64
	ContentRevision *int64 // source table content_revision, used by YT Copy to skip unchanged tables
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

func NewYtTableMeta(cluster, prefix, name string, rows, weight int64, columns []string) *YtTableMeta {
	return NewYtTableMetaWithRevision(cluster, prefix, name, rows, weight, columns, nil)
}

func NewYtTableMetaWithRevision(cluster, prefix, name string, rows, weight int64, columns []string, contentRev *int64) *YtTableMeta {
	return &YtTableMeta{
		Cluster:         cluster,
		Prefix:          prefix,
		Name:            strings.TrimPrefix(name, "/"),
		RowCount:        rows,
		Columns:         columns,
		DataWeight:      weight,
		NodeID:          nil,
		ContentRevision: contentRev,
	}
}

type YtTables []*YtTableMeta
