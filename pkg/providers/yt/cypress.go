package yt

import (
	"context"
	"sort"
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

// ListNodesWithAttrs returns name-sorted list of nodes with attributes based on specified arguments
func ListNodesWithAttrs(ctx context.Context, client yt.CypressClient, path ypath.Path, prefix string, recursive bool) ([]*NodeInfo, error) {
	var nodes []string
	var err error
	if recursive {
		nodes, err = listNodesRecursive(ctx, client, path)
	} else {
		nodes, err = listNodes(ctx, client, path)
	}
	if err != nil {
		return nil, xerrors.Errorf("unable to list nodes(recurive=%v): %w", recursive, err)
	}

	var result []*NodeInfo
	for _, node := range nodes {
		if strings.HasPrefix(node, prefix) {
			nodePath := SafeChild(path, node)
			attrs, err := GetNodeAttrs(ctx, client, nodePath)
			if err != nil {
				return nil, xerrors.Errorf("unable to get node attributes: %w", err)
			}
			result = append(result, NewNodeInfo(node, nodePath, attrs))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result, nil
}

func listNodes(ctx context.Context, client yt.CypressClient, path ypath.Path) ([]string, error) {
	var nodes []struct {
		Name string `yson:",value"`
	}
	if err := client.ListNode(ctx, path, &nodes, nil); err != nil {
		return nil, err
	}
	res := make([]string, len(nodes))
	for i, n := range nodes {
		res[i] = n.Name
	}
	return res, nil
}

func listNodesRecursive(ctx context.Context, client yt.CypressClient, basePath ypath.Path) ([]string, error) {
	res := []string{}
	var nodes []struct {
		Name string      `yson:",value"`
		Type yt.NodeType `yson:"type,attr"`
	}
	nestedMapNodes := []string{""}
	for len(nestedMapNodes) > 0 {
		next := []string{}
		for _, m := range nestedMapNodes {
			if err := client.ListNode(ctx, SafeChild(basePath, m), &nodes, &yt.ListNodeOptions{Attributes: []string{"type"}}); err != nil {
				return nil, xerrors.Errorf("unable to list nodes of %v/%v: %w", basePath, m, err)
			}
			for _, node := range nodes {
				relativePath := SafeChild(ypath.Path(m), node.Name)
				if node.Type == yt.NodeMap {
					next = append(next, string(relativePath))
				} else {
					res = append(res, string(relativePath))
				}
			}
		}
		nodes = nil
		nestedMapNodes = next
	}
	return res, nil
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
