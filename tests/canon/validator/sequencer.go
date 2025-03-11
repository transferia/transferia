package validator

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
)

type sequencer struct {
	rows []abstract.ChangeItem

	flushCallCounter int

	t              *testing.T
	rowMiddlewares []func([]abstract.ChangeItem) []abstract.ChangeItem
}

func (c *sequencer) Close() error {
	return nil
}

func (c *sequencer) Push(items []abstract.ChangeItem) error {
	c.rows = append(c.rows, items...)
	return nil
}

func (c *sequencer) Dump() {
	c.t.Run(fmt.Sprintf("canon_%d", c.flushCallCounter), func(t *testing.T) {
		for _, mw := range c.rowMiddlewares {
			c.rows = mw(c.rows)
		}
		if len(c.rows) > 0 {
			var typedChanges []abstract.TypedChangeItem
			for _, row := range c.rows {
				typedChanges = append(typedChanges, abstract.TypedChangeItem(row))
			}
			rawJSON, err := json.MarshalIndent(typedChanges, "", "    ")
			require.NoError(t, err)
			canon.SaveJSON(t, string(rawJSON))
		}
	})
	c.flushCallCounter += 1
	c.rows = nil
}

func newSequencer(t *testing.T, rowMiddlewares []func([]abstract.ChangeItem) []abstract.ChangeItem) *sequencer {
	return &sequencer{
		rows: nil,

		flushCallCounter: 0,

		t:              t,
		rowMiddlewares: rowMiddlewares,
	}
}

func Sequencer(t *testing.T, rowMiddlewares ...func([]abstract.ChangeItem) []abstract.ChangeItem) (middleware func() abstract.Sinker, dumpCallback func()) {
	result := newSequencer(t, rowMiddlewares)
	return func() abstract.Sinker {
			return result
		}, func() {
			result.Dump()
		}
}
