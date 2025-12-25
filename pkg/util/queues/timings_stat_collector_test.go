package util

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestTimings(t *testing.T) {
	timings := NewTimingsStatCollector()
	tableID := abstract.TablePartID{TableID: abstract.TableID{Namespace: "a", Name: "b"}, PartID: ""}
	timings.Started(tableID)
	timings.FoundWriter(tableID)
	timings.Finished(tableID)
	fields := timings.GetResults()
	require.Len(t, fields, 2)
	require.Equal(t, "find_writer_stat", fields[0].Key())
	require.Equal(t, "write_stat", fields[1].Key())
}
