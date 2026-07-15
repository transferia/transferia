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

var getAttrList = []string{"type", "path", "row_count", "data_weight", "content_revision", "dynamic", "schema", "pivot_keys"}
var linkResolveAttrList = []string{"type", "path", "row_count", "data_weight", "content_revision", "dynamic", "schema", "pivot_keys", "target_path"}

// ListNodes recursively lists nodes of given types under the given paths
func ListNodes(ctx context.Context, y yt.CypressClient, cluster string, paths []string, types []yt.NodeType, skipLinkFollowing bool, logger log.Logger) (YtNodes, error) {
	logger.Debug("Getting list of nodes")
	var result YtNodes

	nodeTypes := set.New[yt.NodeType](types...)

	var traverse func(string, string, int) error
	traverse = func(prefix, path string, depth int) error {
		if depth > 16 {
			return xerrors.Errorf("link chain too deep at %s/%s", prefix, path)
		}
		dirPath := prefix + path
		var outNodes []struct {
			Name            string      `yson:",value"`
			Path            string      `yson:"path,attr"`
			Type            yt.NodeType `yson:"type,attr"`
			RowCount        int64       `yson:"row_count,attr"`
			DataWeight      int64       `yson:"data_weight,attr"`
			ContentRevision int64       `yson:"content_revision,attr,omitempty"`
			Dynamic         bool        `yson:"dynamic,attr"`
			Schema          interface{} `yson:"schema,attr"`
			PivotKeys       interface{} `yson:"pivot_keys,attr"`
		}
		opts := yt.ListNodeOptions{Attributes: getAttrList}
		if err := y.ListNode(ctx, ypath.NewRich(dirPath), &outNodes, &opts); err != nil {
			return xerrors.Errorf("cannot list node %s: %v", dirPath, err)
		}
		for _, node := range outNodes {
			nodeRelPath := path + "/" + node.Name
			if node.Type == yt.NodeMap {
				logger.Debugf("Traversing node %s from %s", nodeRelPath, prefix)
				if err := traverse(prefix, nodeRelPath, depth); err != nil {
					return xerrors.Errorf("error traversing %s: %w", nodeRelPath, err)
				}
			} else if node.Type == yt.NodeLink {
				if skipLinkFollowing {
					logger.Warnf("Skipping link %s (skip_link_following enabled)", prefix+nodeRelPath)
					continue
				}
				linkPath := prefix + nodeRelPath
				var resolved struct {
					Type            yt.NodeType `yson:"type,attr"`
					Path            string      `yson:"path,attr"`
					RowCount        int64       `yson:"row_count,attr"`
					DataWeight      int64       `yson:"data_weight,attr"`
					ContentRevision int64       `yson:"content_revision,attr"`
					Dynamic         bool        `yson:"dynamic,attr"`
					Schema          interface{} `yson:"schema,attr"`
					PivotKeys       interface{} `yson:"pivot_keys,attr"`
				}
				resolveOpts := yt.GetNodeOptions{Attributes: linkResolveAttrList}
				if err := y.GetNode(ctx, ypath.NewRich(linkPath), &resolved, &resolveOpts); err != nil {
					return xerrors.Errorf("cannot resolve link %s: %w", linkPath, err)
				}
				logger.Infof("Resolved link %s -> %s (type %s)", linkPath, resolved.Path, resolved.Type)
				if resolved.Type == yt.NodeMap {
					if err := traverse(prefix, nodeRelPath, depth+1); err != nil {
						return xerrors.Errorf("error traversing link target %s: %w", nodeRelPath, err)
					}
				} else if nodeTypes.Empty() || nodeTypes.Contains(resolved.Type) {
					logger.Infof("Found %s %s (via link) from %s, %d rows weighting %s", resolved.Type, nodeRelPath, prefix, resolved.RowCount, humanize.Bytes(uint64(resolved.DataWeight)))
					result = append(result, NewYtNodeMeta(cluster, prefix, nodeRelPath, resolved.RowCount, resolved.DataWeight, resolved.ContentRevision, resolved.Type, resolved.Dynamic, resolved.Schema, resolved.PivotKeys))
				} else {
					logger.Warnf("Link %s resolves to unsupported type %s, skipping", linkPath, resolved.Type)
				}
			} else if nodeTypes.Empty() || nodeTypes.Contains(node.Type) {
				logger.Infof("Found %s %s from %s, %d rows weighting %s", node.Type, nodeRelPath, prefix, node.RowCount, humanize.Bytes(uint64(node.DataWeight)))
				result = append(result, NewYtNodeMeta(cluster, prefix, nodeRelPath, node.RowCount, node.DataWeight, node.ContentRevision, node.Type, node.Dynamic, node.Schema, node.PivotKeys))
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
			Dynamic         bool        `yson:"dynamic,attr"`
			Schema          interface{} `yson:"schema,attr"`
			PivotKeys       interface{} `yson:"pivot_keys,attr"`
		}
		opts := yt.GetNodeOptions{Attributes: getAttrList}
		if err := y.GetNode(ctx, yp, &attrs, &opts); err != nil {
			return nil, xerrors.Errorf("cannot get yt node %s: %w", p, err)
		}
		if attrs.Type == yt.NodeMap {
			logger.Debugf("Traversing %s", p)
			if err := traverse(attrs.Path, "", 0); err != nil {
				return nil, xerrors.Errorf("unable to traverse path %s: %w", p, err)
			}
		} else if nodeTypes.Empty() || nodeTypes.Contains(attrs.Type) {
			pref, name, err := ypath.Split(yp.YPath())
			if err != nil {
				return nil, xerrors.Errorf("error splitting path %s: %w", p, err)
			}
			logger.Infof("Adding %s %s from %s", attrs.Type, name, pref.String())
			result = append(result, NewYtNodeMeta(cluster, pref.String(), name, attrs.RowCount, attrs.DataWeight, attrs.ContentRevision, attrs.Type, attrs.Dynamic, attrs.Schema, attrs.PivotKeys))
		} else {
			return nil, xerrors.Errorf("cypress path %s is %s, not a supported node type (table, dynamic table, file, link) or map_node", p, attrs.Type)
		}
	}
	return result, nil
}
