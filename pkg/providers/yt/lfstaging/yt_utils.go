//go:build !disable_yt_provider

package lfstaging

import (
	"context"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type ytLockData struct {
	AttributeKey string `yson:"attribute_key"`
}

type ytNode struct {
	Name string `yson:",value"`
	Type string `yson:"type,attr"`
	Path string `yson:"path,attr"`

	WriterLock int64        `yson:"_lfstaging_writer_lock,attr"`
	Locks      []ytLockData `yson:"locks,attr"`

	IsWriterFinished bool
}

func listNodes(client yt.CypressClient, path ypath.Path) ([]ytNode, error) {
	var nodes []ytNode
	err := client.ListNode(
		context.Background(),
		path,
		&nodes,
		&yt.ListNodeOptions{Attributes: []string{"type", "path", writerLockAttr, "locks"}},
	)

	for i, node := range nodes {
		containsLock := false
		for _, lock := range node.Locks {
			if lock.AttributeKey == writerLockAttr {
				containsLock = true
			}
		}

		nodes[i].IsWriterFinished = !containsLock && node.WriterLock == 1
	}

	if err != nil {
		return nil, err
	} else {
		return nodes, nil
	}
}
