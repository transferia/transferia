//go:build !disable_postgres_provider

package postgres

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

var (
	// Test input {{{
	changesets = [][]byte{[]byte(`{
		"xid": 47931104,
		"nextlsn": "6E/85004178",
		"timestamp": "2019-07-30 11:37:13.107342+00",
		"change": [
			{
				"kind": "update",
				"schema": "public",
				"table": "pgbench_accounts",
				"columnnames": [
					"aid",
					"bid",
					"abalance",
					"filler"
				],
				"columntypes": [
					"integer",
					"integer",
					"integer",
					"character(84)"
				],
				"columnvalues": [
					90651,
					1,
					689,
					"asdasd"
				],
				"oldkeys": {
					"keynames": [
						"aid"
					],
					"keytypes": [
						"integer"
					],
					"keyvalues": [
						90651
					]
				}
			}
		]
	}`), []byte(`{
		"xid": 123,
		"nextlsn": "6E/85004179",
		"timestamp": "2020-08-31 10:20:30.405060+00",
		"change": [
			{
				"kind": "update",
				"schema": "public",
				"table": "kek",
				"columnnames": [
					"id"
				],
				"columntypes": [
					"integer"
				],
				"columnvalues": [
					777
				],
				"oldkeys": {
					"keynames": [
						"id"
					],
					"keytypes": [
						"integer"
					],
					"keyvalues": [
						666
					]
				}
			},
			{
				"kind": "update",
				"schema": "private",
				"table": "lel",
				"columnnames": [
					"name"
				],
				"columntypes": [
					"varchar"
				],
				"columnvalues": [
					"new text"
				],
				"oldkeys": {
					"keynames": [
						"name"
					],
					"keytypes": [
						"varchar"
					],
					"keyvalues": [
						"old text"
					]
				}
			}
		]
	}`)}  // }}}

	// Test canonical data {{{
	multipleChangesetsCanonical = []Wal2JSONItem{
		{
			ID:         47931104,
			CommitTime: 1564486633107342000,
			Kind:       "update",
			Schema:     "public",
			Table:      "pgbench_accounts",
			ColumnNames: []string{
				"aid",
				"bid",
				"abalance",
				"filler",
			},
			ColumnValues: []interface{}{
				json.Number("90651"),
				json.Number("1"),
				json.Number("689"),
				"asdasd",
			},
			OldKeys: OldKeysType{
				OldKeysType: abstract.OldKeysType{
					KeyNames: []string{
						"aid",
					},
					KeyTypes: []string{
						"integer",
					},
					KeyValues: []interface{}{
						json.Number("90651"),
					},
				},
			},
			Size: abstract.RawEventSize(uint64(482)),
		},
		{
			ID:         123,
			CommitTime: 1598869230405060000,
			Kind:       "update",
			Schema:     "public",
			Table:      "kek",
			ColumnNames: []string{
				"id",
			},
			ColumnValues: []interface{}{
				json.Number("777"),
			},
			OldKeys: OldKeysType{
				OldKeysType: abstract.OldKeysType{
					KeyNames: []string{
						"id",
					},
					KeyTypes: []string{
						"integer",
					},
					KeyValues: []interface{}{
						json.Number("666"),
					},
				},
			},
			Size: abstract.RawEventSize(uint64(332)),
		},
		{
			ID:         123,
			CommitTime: 1598869230405060000,
			Kind:       "update",
			Schema:     "private",
			Table:      "lel",
			ColumnNames: []string{
				"name",
			},
			ColumnValues: []interface{}{
				"new text",
			},
			OldKeys: OldKeysType{
				OldKeysType: abstract.OldKeysType{
					KeyNames: []string{
						"name",
					},
					KeyTypes: []string{
						"varchar",
					},
					KeyValues: []interface{}{
						"old text",
					},
				},
			},
			Size: abstract.RawEventSize(uint64(356)),
		},
	} // }}}
)

func TestWal2JsonEntireChunk(t *testing.T) {
	chunk := append(changesets[0], changesets[1]...)
	parser := NewWal2JsonParser()
	defer parser.Close()
	changes, err := parser.Parse(chunk)
	require.NoError(t, err, "Cannot parse test changesets")
	require.Equal(t, len(multipleChangesetsCanonical), len(changes), "Changes' lengths do not match")
	for i := 0; i < len(multipleChangesetsCanonical); i++ {
		require.Equal(t, multipleChangesetsCanonical[i], *changes[i], "Item %d differs from the canonical one", i)
	}
}

func TestWal2JsonLineByLine(t *testing.T) {
	chunk := append(changesets[0], changesets[1]...)
	parser := NewWal2JsonParser()
	defer parser.Close()
	scanner := bufio.NewScanner(bytes.NewReader(chunk))
	scanner.Split(scanUnstrippedLines)

	feedLines := func(start, end int, expectedChange Wal2JSONItem) {
		var i int
		errmsg := func(msg string) string {
			return fmt.Sprintf("Line %d: %s", i, msg)
		}
		for i = start; i < end-1; i++ {
			ok := scanner.Scan()
			require.Equal(t, true, ok)

			items, err := parser.Parse(scanner.Bytes())
			require.NoError(t, err, errmsg("Cannot parse input"))
			require.Equal(t, []*Wal2JSONItem(nil), items, errmsg("Unexpected change"))
		}

		ok := scanner.Scan()
		require.Equal(t, true, ok)

		items, err := parser.Parse(scanner.Bytes())
		require.NoError(t, err, errmsg("Cannot parse input"))
		require.Equal(t, 1, len(items), errmsg("Changes' lengths do not match"))
		require.Equal(t, expectedChange, *items[0], errmsg("Changes do not match"))
	}

	feedLines(0, 39, multipleChangesetsCanonical[0])
	feedLines(39, 70, multipleChangesetsCanonical[1])
	feedLines(70, 95, multipleChangesetsCanonical[2])

	for scanner.Scan() {
		items, err := parser.Parse(scanner.Bytes())
		require.NoError(t, err, "Cannot parse data after line 95")
		require.Equal(t, []*Wal2JSONItem(nil), items, "Expected no items after line 95")
	}
}

func TestWal2JsonError(t *testing.T) {
	chunk := append(changesets[0], changesets[1]...)
	parser := NewWal2JsonParser()
	defer parser.Close()

	changes, err := parser.Parse([]byte{})
	require.NoError(t, err, "No error expected on parsing empty data slice")
	require.Equal(t, []*Wal2JSONItem(nil), changes, "No changes expected after empty input")

	changes, err = parser.Parse(chunk)
	require.NoError(t, err, "Cannot parse test changesets")

	require.Equal(t, len(multipleChangesetsCanonical), len(changes), "Changes' lengths do not match")
	for i := 0; i < len(multipleChangesetsCanonical); i++ {
		require.Equal(t, multipleChangesetsCanonical[i], *changes[i], "Item %d differs from the canonical one", i)
	}

	changes, err = parser.Parse([]byte("kek"))
	require.Equal(t, []*Wal2JSONItem(nil), changes, "No items expected after erroneous input")
	require.NotEqual(t, error(nil), err, "Error must not be nil after erroneous input")

	changes, err = parser.Parse([]byte("lel"))
	require.Equal(t, []*Wal2JSONItem(nil), changes, "No items expected after erroneous input")
	require.NotEqual(t, error(nil), err, "Error must not be nil after erroneous input")

	changes, err = parser.Parse([]byte{})
	require.Equal(t, []*Wal2JSONItem(nil), changes, "No items expected after erroneous input")
	require.NotEqual(t, error(nil), err, "Error must not be nil after erroneous input")
}

func scanUnstrippedLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[0 : i+1], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

type CyclicReader struct {
	buf []byte
	pos int
}

func (r *CyclicReader) Read(b []byte) (n int, err error) {
	if r.pos == len(r.buf) {
		// Wrap around
		r.pos = 0
	}

	copyFrom := r.buf[r.pos:]
	if len(b) < len(copyFrom) {
		n = len(b)
	} else {
		n = len(copyFrom)
	}
	copyFrom = copyFrom[:n]
	r.pos += n
	return copy(b, copyFrom), nil
}
