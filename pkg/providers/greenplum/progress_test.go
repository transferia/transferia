//go:build !disable_greenplum_provider

package greenplum

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

type ProgressTracker struct {
	Current  uint64
	Progress uint64
	Total    uint64
}

func NewProgressTracker() *ProgressTracker {
	return &ProgressTracker{
		Current:  0,
		Progress: 0,
		Total:    0,
	}
}

func (pt *ProgressTracker) ProgressFn() abstract.LoadProgress {
	return func(current uint64, progress uint64, total uint64) {
		pt.Current = current
		pt.Progress = progress
		pt.Total = total
	}
}

func (pt *ProgressTracker) Percentage() float64 {
	return (float64(pt.Progress) / float64(pt.Total)) * 100
}

const (
	// SampleCurrent is just an arbitrary value. It must not matter.
	SampleCurrent uint64 = 3751
)

const (
	// TotalTest is the number of rows in the current partition
	TotalTest uint64 = 12
	// EtaRowTest is the total number of rows (in all partitions)
	EtaRowTest uint64 = 120
)

/*
 * What is the meaning of these tests?
 * `underTest()` calls obtain in-partition progress.
 * The function under test must convert the percentage of completeness of a given partition into the total progress (among all partitions).
 *
 * Note that `SampleCurrent` value does not matter for progress tracking.
 */

func TestParts1Completed0Progress0(t *testing.T) {
	trk := NewProgressTracker()
	underTest := ComposePartialProgressFn(trk.ProgressFn(), 0, 1, EtaRowPartialProgress)
	underTest(SampleCurrent, 0, TotalTest)

	require.Equal(t, 0.0, trk.Percentage())
	require.Equal(t, uint64(0), trk.Progress)
	require.Equal(t, uint64(EtaRowPartialProgress), trk.Total)
}

func TestParts2Completed0Progress0(t *testing.T) {
	trk := NewProgressTracker()
	underTest := ComposePartialProgressFn(trk.ProgressFn(), 0, 2, EtaRowPartialProgress)
	underTest(SampleCurrent, 0, TotalTest)

	require.Equal(t, 0.0, trk.Percentage())
	require.Equal(t, uint64(0), trk.Progress)
	require.Equal(t, uint64(EtaRowPartialProgress), trk.Total)
}

func TestParts3Completed0Progress0(t *testing.T) {
	trk := NewProgressTracker()
	underTest := ComposePartialProgressFn(trk.ProgressFn(), 0, 3, EtaRowPartialProgress)
	underTest(SampleCurrent, 0, TotalTest)

	require.Equal(t, 0.0, trk.Percentage())
	require.Equal(t, uint64(0), trk.Progress)
	require.Equal(t, uint64(EtaRowPartialProgress), trk.Total)
}

func TestParts1048576Completed0Progress0(t *testing.T) {
	trk := NewProgressTracker()
	underTest := ComposePartialProgressFn(trk.ProgressFn(), 0, 1048576, EtaRowPartialProgress)
	underTest(SampleCurrent, 0, TotalTest)

	require.Equal(t, 0.0, trk.Percentage())
	require.Equal(t, uint64(0), trk.Progress)
	require.Equal(t, uint64(EtaRowPartialProgress), trk.Total)
}

func TestParts2Completed1Progress100(t *testing.T) {
	trk := NewProgressTracker()
	underTest := ComposePartialProgressFn(trk.ProgressFn(), 1, 2, EtaRowTest)
	underTest(SampleCurrent, TotalTest, TotalTest)

	require.Equal(t, 100.0, trk.Percentage())
	require.Equal(t, EtaRowTest, trk.Progress)
	require.Equal(t, EtaRowTest, trk.Total)
}

func TestParts3Completed1Progress50(t *testing.T) {
	trk := NewProgressTracker()
	underTest := ComposePartialProgressFn(trk.ProgressFn(), 1, 3, EtaRowTest)
	underTest(SampleCurrent, TotalTest/2, TotalTest)

	require.Equal(t, 50.0, trk.Percentage())
	require.Equal(t, EtaRowTest/2, trk.Progress)
	require.Equal(t, EtaRowTest, trk.Total)
}

func TestParts20Completed1Progress7half(t *testing.T) {
	trk := NewProgressTracker()
	underTest := ComposePartialProgressFn(trk.ProgressFn(), 1, 20, EtaRowTest)
	underTest(SampleCurrent, TotalTest/2, TotalTest)

	require.Equal(t, 7.5, trk.Percentage())
	require.Equal(t, uint64(9), trk.Progress)
	require.Equal(t, EtaRowTest, trk.Total)
}
