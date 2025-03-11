package validator

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
)

type initDone struct {
	snapshots       map[abstract.TableID]bool
	snapshotSchemas map[abstract.TableID]*abstract.TableSchema

	t *testing.T
}

func (c *initDone) Close() error {
	c.snapshots = map[abstract.TableID]bool{}
	c.snapshotSchemas = map[abstract.TableID]*abstract.TableSchema{}
	return nil
}

func (c *initDone) Push(items []abstract.ChangeItem) error {
	for _, item := range items {
		if item.Kind == abstract.InitTableLoad {
			c.snapshots[item.TableID()] = true
			c.snapshotSchemas[item.TableID()] = item.TableSchema
			require.NotNil(c.t, item.TableSchema)
		}
		if item.Kind == abstract.DoneTableLoad {
			require.True(c.t, c.snapshots[item.TableID()])
			require.Equal(c.t, c.snapshotSchemas[item.TableID()], item.TableSchema)
			c.snapshots[item.TableID()] = false
		}
		if item.Kind == abstract.InsertKind {
			if len(c.snapshots) == 0 {
				return nil // no snapshot, exit
			}
			inited, exist := c.snapshots[item.TableID()]
			require.Equal(c.t, c.snapshotSchemas[item.TableID()], item.TableSchema)
			require.Truef(c.t, exist, "table is not known in snapshot")
			require.Truef(c.t, inited, "table is not opened by init event")
		}
	}
	return nil
}

func newInitDone(t *testing.T) *initDone {
	return &initDone{
		snapshots:       map[abstract.TableID]bool{},
		snapshotSchemas: map[abstract.TableID]*abstract.TableSchema{},
		t:               t,
	}
}

func InitDone(t *testing.T) func() abstract.Sinker {
	result := newInitDone(t)
	return func() abstract.Sinker {
		return result
	}
}
