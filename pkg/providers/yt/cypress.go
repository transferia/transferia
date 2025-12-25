package yt

import (
	"context"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type NodeAttrs struct {
	Type        yt.NodeType   `yson:"type"`
	Dynamic     bool          `yson:"dynamic"`
	TabletState string        `yson:"tablet_state"`
	Schema      schema.Schema `yson:"schema"`
	OptimizeFor string        `yson:"optimize_for"`
	Atomicity   string        `yson:"atomicity"`
}

type NodeInfo struct {
	Name  string
	Path  ypath.Path
	Attrs *NodeAttrs
}

func NewNodeInfo(name string, path ypath.Path, attrs *NodeAttrs) *NodeInfo {
	return &NodeInfo{Name: name, Path: path, Attrs: attrs}
}

func GetNodeInfo(ctx context.Context, client yt.CypressClient, path ypath.Path) (*NodeInfo, error) {
	attrs, err := GetNodeAttrs(ctx, client, path)
	if err != nil {
		return nil, xerrors.Errorf("unable to get node attributes: %w", err)
	}
	return NewNodeInfo("", path, attrs), nil
}

func GetNodeAttrs(ctx context.Context, client yt.CypressClient, path ypath.Path) (*NodeAttrs, error) {
	attrs := new(NodeAttrs)
	if err := client.GetNode(ctx, path.Attrs(), &attrs, &yt.GetNodeOptions{
		Attributes: []string{"type", "dynamic", "tablet_state", "schema", "optimize_for", "atomicity"}}); err != nil {
		return nil, xerrors.Errorf("unable to get node: %w", err)
	}
	return attrs, nil
}

// SafeChild appends children to path. It works like path.Child(child) with exceptions.
// this method assumes:
//  1. ypath object is correct, i.e. no trailing path delimiter symbol exists
//
// This method guarantees:
//  1. YPath with appended children has deduplicated path delimiters in appended string and
//     no trailing path delimiter would be presented.
//  2. TODO(@kry127) TM-6290 not yet guaranteed, but nice to have: special symbols should be replaced
func SafeChild(path ypath.Path, children ...string) ypath.Path {
	unrefinedRelativePath := strings.Join(children, "/")
	relativePath := relativePathSuitableForYPath(unrefinedRelativePath)
	if len(relativePath) > 0 {
		if path == "" {
			return ypath.Path(relativePath)
		}
		return path.Child(relativePath)
	}
	return path
}

// relativePathSuitableForYPath processes relativeYPath in order to make it correct for appending
// to correct YPath.
func relativePathSuitableForYPath(relativePath string) string {
	tokens := strings.Split(relativePath, "/")
	nonEmptyTokens := yslices.Filter(tokens, func(token string) bool {
		return len(token) > 0
	})
	deduplicatedSlashes := strings.Join(nonEmptyTokens, "/")
	// TODO(@kry127) TM-6290 add symbol escaping here
	return deduplicatedSlashes
}
