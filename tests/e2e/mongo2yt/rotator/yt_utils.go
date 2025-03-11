package rotator

import (
	"context"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type ytNode struct {
	Name string      `yson:",value"`
	Type yt.NodeType `yson:"type,attr"`
	Path ypath.Path  `yson:"path,attr"`
}

func recursiveListYTNode(ctx context.Context, client yt.Client, path ypath.Path) ([]ytNode, error) {
	var nodes []ytNode
	err := client.ListNode(
		ctx,
		path,
		&nodes,
		&yt.ListNodeOptions{Attributes: []string{"type"}},
	)
	if err != nil {
		return nil, err
	}

	var result []ytNode
	for _, node := range nodes {
		if node.Type == yt.NodeMap {
			recursiveNodes, err := recursiveListYTNode(ctx, client, path.Child(node.Name))
			if err != nil {
				return nil, err
			}

			for _, recNode := range recursiveNodes {
				recNode.Path = path
				recNode.Name = string(ypath.Path(node.Name).Child(recNode.Name))
				result = append(result, recNode)
			}
			continue
		}
		result = append(result, node)
	}
	return result, nil
}

func tablePrinter(ctx context.Context, client yt.Client, path ypath.Path) ([]map[string]interface{}, error) {
	reader, err := client.ReadTable(ctx, path, nil)
	if err != nil {
		return nil, xerrors.Errorf("error opening yt table reader: %w", err)
	}

	var result []map[string]interface{}
	for reader.Next() {
		row := new(map[string]interface{})
		err = reader.Scan(row)
		if err != nil {
			return nil, xerrors.Errorf("scan error: %w", err)
		}
		result = append(result, *row)
	}
	if reader.Err() != nil {
		return nil, xerrors.Errorf("reader error: %w", reader.Err())
	}
	return result, nil
}
