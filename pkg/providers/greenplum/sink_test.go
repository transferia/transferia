//go:build !disable_greenplum_provider

package greenplum

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type fakePgSinks struct {
	pgSinksImpl
	counter int
}

func (s *fakePgSinks) PGSink(cts context.Context, sp GPSegPointer, sinkParams PgSinkParamsRegulated) (abstract.Sinker, error) {
	s.mutex.Lock()
	s.counter++
	s.mutex.Unlock()
	for {
		s.mutex.Lock()
		if s.counter >= 2 {
			break
		}
		s.mutex.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
	s.mutex.Unlock()

	return nil, xerrors.Errorf("failed to create PostgreSQL sink object")
}

func makeTestChangeItem(t *testing.T, colNames []string, colValues []interface{}, isKey []bool, kind abstract.Kind) abstract.ChangeItem {
	require.Equal(t, len(colValues), len(colNames))
	require.Equal(t, len(colValues), len(isKey))
	var schema []abstract.ColSchema
	for i := 0; i < len(colNames); i++ {
		schema = append(schema, abstract.ColSchema{PrimaryKey: isKey[i], ColumnName: colNames[i]})
	}
	return abstract.ChangeItem{
		ColumnNames:  colNames,
		ColumnValues: colValues,
		TableSchema:  abstract.NewTableSchema(schema),
		Kind:         kind,
	}
}

// TestGpParallel checks that sink pushes values to segments asynchronously when destination has more than 12 segments
// The value of required segments is due to the SegPoolShare field of sink
func TestGpParallel(t *testing.T) {
	fakeSinks := new(fakePgSinks)
	fakeSinks.totalSegmentsCached = 13

	sink := new(Sink)
	sink.atReplication = false
	sink.sinks = fakeSinks
	sink.rowChangeItems = make(map[GPSegPointer][]abstract.ChangeItem)
	sink.sinkParams = new(PgSinkParamsRegulated)
	sink.SegPoolShare = 0.166

	err := sink.Push([]abstract.ChangeItem{
		makeTestChangeItem(t, []string{"col1"}, []interface{}{"test"}, []bool{false}, abstract.InsertKind),
		makeTestChangeItem(t, []string{"col1"}, []interface{}{"test"}, []bool{false}, abstract.InsertKind),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create PostgreSQL sink object")
	require.Equal(t, fakeSinks.counter, 2)
}
