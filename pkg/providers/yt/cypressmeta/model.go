package cypressmeta

import (
	"strings"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type YtNodeMeta struct {
	Cluster         string
	Prefix          string
	Name            string
	RowCount        int64
	NodeID          *yt.NodeID
	DataWeight      int64
	ContentRevision int64 // source table content_revision, used by YT Copy to skip unchanged tables
	NodeType        yt.NodeType
}

func (t *YtNodeMeta) FullName() string {
	return t.Cluster + "." + t.OriginalPath()
}

func (t *YtNodeMeta) OriginalPath() string {
	return t.Prefix + "/" + t.Name
}

func (t *YtNodeMeta) OriginalYPath() ypath.YPath {
	return ypath.NewRich(t.OriginalPath())
}

func NewYtNodeMeta(cluster, prefix, name string, rows, weight int64, contentRev int64, nodeType yt.NodeType) *YtNodeMeta {
	return &YtNodeMeta{
		Cluster:         cluster,
		Prefix:          prefix,
		Name:            strings.TrimPrefix(name, "/"),
		RowCount:        rows,
		DataWeight:      weight,
		NodeID:          nil,
		ContentRevision: contentRev,
		NodeType:        nodeType,
	}
}

type YtNodes []*YtNodeMeta
