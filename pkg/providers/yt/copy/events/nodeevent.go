package events

import (
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/providers/yt/cypressmeta"
)

type nodeEvent struct {
	path *cypressmeta.YtNodeMeta
}

type NodeEvent interface {
	abstract2.Event
	Node() *cypressmeta.YtNodeMeta
}

func (t *nodeEvent) Node() *cypressmeta.YtNodeMeta {
	return t.path
}

func newNodeEvent(path *cypressmeta.YtNodeMeta) NodeEvent {
	return &nodeEvent{path}
}
