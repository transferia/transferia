package cypressmeta

import (
	"context"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

// TODO: look at go.ytsaurus.tech/yt/go/ytwalk, maybe replace/use

var getAttrList = []string{"type", "path", "row_count", "data_weight", "content_revision"}

// ListNodes recursively lists nodes of given types under the given paths
func ListNodes(ctx context.Context, y yt.CypressClient, cluster string, paths []string, types []yt.NodeType, logger log.Logger) (YtNodes, error) {
	logger.Debug("Getting list of nodes")
	var result YtNodes

	nodeTypes := set.New[yt.NodeType](types...)

	var traverse func(string, string) error
	traverse = func(prefix, path string) error {
		dirPath := prefix + path
		var outNodes []struct {
			Name            string      `yson:",value"`
			Path            string      `yson:"path,attr"`
			Type            yt.NodeType `yson:"type,attr"`
			RowCount        int64       `yson:"row_count,attr"`
			DataWeight      int64       `yson:"data_weight,attr"`
			ContentRevision int64       `yson:"content_revision,attr,omitempty"`
		}
		opts := yt.ListNodeOptions{Attributes: getAttrList}
		if err := y.ListNode(ctx, ypath.NewRich(dirPath), &outNodes, &opts); err != nil {
			return xerrors.Errorf("cannot list node %s: %v", dirPath, err)
		}
		for _, node := range outNodes {
			nodeRelPath := path + "/" + node.Name
			if node.Type == yt.NodeMap {
				logger.Debugf("Traversing node %s from %s", nodeRelPath, prefix)
				if err := traverse(prefix, nodeRelPath); err != nil {
					return err
				}
			} else if nodeTypes.Empty() || nodeTypes.Contains(node.Type) {
				logger.Infof("Found %s %s from %s, %d rows weighting %s", node.Type, nodeRelPath, prefix, node.RowCount, humanize.Bytes(uint64(node.DataWeight)))
				result = append(result, NewYtNodeMeta(cluster, prefix, nodeRelPath, node.RowCount, node.DataWeight, node.ContentRevision, node.Type))
			} else {
				logger.Warnf("Node %s has unsupported type %s, skipping", node.Path, node.Type)
			}
		}
		return nil
	}

	for _, p := range paths {
		yp, err := ypath.Parse(p)
		if err != nil {
			return nil, xerrors.Errorf("cannot parse input ypath %s: %w", p, err)
		}
		var attrs struct {
			Path            string      `yson:"path,attr"`
			Type            yt.NodeType `yson:"type,attr"`
			RowCount        int64       `yson:"row_count,attr"`
			DataWeight      int64       `yson:"data_weight,attr"`
			ContentRevision int64       `yson:"content_revision,attr"`
		}
		opts := yt.GetNodeOptions{Attributes: getAttrList}
		if err := y.GetNode(ctx, yp, &attrs, &opts); err != nil {
			return nil, xerrors.Errorf("cannot get yt node %s: %w", p, err)
		}
		if attrs.Type == yt.NodeMap {
			logger.Debugf("Traversing %s", p)
			if err := traverse(attrs.Path, ""); err != nil {
				return nil, xerrors.Errorf("unable to traverse path %s: %w", p, err)
			}
		} else if nodeTypes.Empty() || nodeTypes.Contains(attrs.Type) {
			pref, name, err := ypath.Split(yp.YPath())
			if err != nil {
				return nil, xerrors.Errorf("error splitting path %s: %w", p, err)
			}
			logger.Infof("Adding %s %s from %s", attrs.Type, name, pref.String())
			result = append(result, NewYtNodeMeta(cluster, pref.String(), name, attrs.RowCount, attrs.DataWeight, attrs.ContentRevision, attrs.Type))
		} else {
			return nil, xerrors.Errorf("cypress path %s is %s, not a supported node type (table, file) or map_node", p, attrs.Type)
		}
	}
	return result, nil
}
