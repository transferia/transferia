package tablemeta

import (
	"context"
	"slices"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

// TODO: look at go.ytsaurus.tech/yt/go/ytwalk, maybe replace/use

var getAttrList = []string{"type", "path", "row_count", "data_weight"}

// ContentRevisionAttr is YT table attribute used by transfer to skip copying unchanged tables during yt_copy.
// https://st.yandex-team.ru/YTADMINREQ-54593.
const ContentRevisionAttr = "content_revision"

// ListTables lists tables under the given paths. When extraAttrs contains ContentRevisionAttr,
// each table's content_revision is fetched and set in YtTableMeta (for YT Copy skip-unchanged logic).
func ListTables(ctx context.Context, y yt.CypressClient, cluster string, paths []string, logger log.Logger, extraAttrs ...string) (YtTables, error) {
	attrList := getAttrList
	wantContentRevision := false
	if slices.Contains(extraAttrs, ContentRevisionAttr) {
		attrList = append(slices.Clone(attrList), ContentRevisionAttr)
		wantContentRevision = true
	}

	logger.Debug("Getting list of tables")
	var result YtTables

	var traverse func(string, string) error
	traverse = func(prefix, path string) error {
		dirPath := prefix + path
		var outNodes []struct {
			Name            string `yson:",value"`
			Path            string `yson:"path,attr"`
			Type            string `yson:"type,attr"`
			RowCount        int64  `yson:"row_count,attr"`
			DataWeight      int64  `yson:"data_weight,attr"`
			ContentRevision *int64 `yson:"content_revision,attr,omitempty"`
		}
		opts := yt.ListNodeOptions{Attributes: attrList}
		if err := y.ListNode(ctx, ypath.NewRich(dirPath), &outNodes, &opts); err != nil {
			return xerrors.Errorf("cannot list node %s: %v", dirPath, err)
		}
		for _, node := range outNodes {
			nodeRelPath := path + "/" + node.Name
			switch node.Type {
			case "table":
				logger.Infof("Found table %s from %s, %d rows weighting %s", nodeRelPath, prefix, node.RowCount, humanize.Bytes(uint64(node.DataWeight)))
				var rev *int64
				if wantContentRevision && node.ContentRevision != nil {
					rev = node.ContentRevision
				}
				result = append(result, NewYtTableMetaWithRevision(cluster, prefix, nodeRelPath, node.RowCount, node.DataWeight, []string{}, rev))
			case "map_node":
				logger.Debugf("Traversing node %s from %s", nodeRelPath, prefix)
				if err := traverse(prefix, nodeRelPath); err != nil {
					return err
				}
			default:
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
			Path            string `yson:"path,attr"`
			Type            string `yson:"type,attr"`
			RowCount        int64  `yson:"row_count,attr"`
			DataWeight      int64  `yson:"data_weight,attr"`
			ContentRevision *int64 `yson:"content_revision,attr,omitempty"`
		}
		opts := yt.GetNodeOptions{Attributes: attrList}
		if err := y.GetNode(ctx, yp, &attrs, &opts); err != nil {
			return nil, xerrors.Errorf("cannot get yt node %s: %w", p, err)
		}
		switch attrs.Type {
		case "table":
			pref, name, err := ypath.Split(yp.YPath())
			if err != nil {
				return nil, xerrors.Errorf("error splitting path %s: %w", p, err)
			}
			logger.Infof("Adding table %s from %s", name, pref.String())
			var rev *int64
			if wantContentRevision && attrs.ContentRevision != nil {
				rev = attrs.ContentRevision
			}
			result = append(result, NewYtTableMetaWithRevision(cluster, pref.String(), name, attrs.RowCount, attrs.DataWeight, yp.Columns, rev))
		case "map_node":
			logger.Debugf("Traversing %s", p)
			if err := traverse(attrs.Path, ""); err != nil {
				return nil, xerrors.Errorf("unable to traverse path %s: %w", p, err)
			}
		default:
			return nil, xerrors.Errorf("cypress path %s is %s, not map_node or table", p, attrs.Type)
		}
	}
	return result, nil
}
