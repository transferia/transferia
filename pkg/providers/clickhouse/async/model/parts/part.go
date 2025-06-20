//go:build !disable_clickhouse_provider

package parts

import (
	"github.com/transferia/transferia/pkg/abstract"
)

// Parts is a collection of table parts
type Parts interface {
	Add(id abstract.TablePartID, part Part)
	Part(id abstract.TablePartID) Part
}

type Part interface {
	Append(rows []abstract.ChangeItem) error
	Commit() error
	Close() error
}

type PartMap map[abstract.TablePartID]Part

func (p PartMap) Add(id abstract.TablePartID, part Part) {
	p[id] = part
}

func (p PartMap) Part(id abstract.TablePartID) Part {
	return p[id]
}
