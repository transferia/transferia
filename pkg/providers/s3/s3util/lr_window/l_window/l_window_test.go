package l_window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/ordered_multimap"
)

func TestIsOutOfRWindow(t *testing.T) {
	startTime := 10 * time.Minute
	windowSize := time.Minute
	var err error

	beforeLabel0 := startTime.Nanoseconds() - 1
	label0 := startTime.Nanoseconds()
	label1 := startTime.Nanoseconds() + 1
	label2 := startTime.Nanoseconds() + 2
	label3 := startTime.Nanoseconds() + 3
	afterLabel3 := startTime.Nanoseconds() + 4

	window := NewLWindow(0)
	err = window.Commit(label0, "A", windowSize, nil)
	require.NoError(t, err)
	err = window.Commit(label0, "B", windowSize, nil)
	require.NoError(t, err)
	err = window.Commit(label0, "C", windowSize, nil)
	require.NoError(t, err)
	err = window.Commit(label0, "D", windowSize, nil)
	require.NoError(t, err)

	require.False(t, window.IsNewFile(beforeLabel0, "E"))

	require.True(t, window.IsNewFile(label0, "F")) // in window, unknown
	require.True(t, window.IsNewFile(label1, "G")) // in window, unknown
	require.True(t, window.IsNewFile(label3, "H")) // in window, unknown

	require.True(t, window.IsNewFile(afterLabel3, "I"))

	require.True(t, window.IsNewFile(label1, "J"))  // in window on known ts, unknown
	require.True(t, window.IsNewFile(label2, "K"))  // in window on unknown ts, unknown
	require.False(t, window.IsNewFile(label3, "D")) // in window, known
}

func TestLWindowIsNewFileWithExistingTimestamp(t *testing.T) {
	w := NewLWindow(0)

	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)

	isNew := w.IsNewFile(100, "file2.txt")
	require.True(t, isNew, "File with same timestamp but different name should be new")

	isNew = w.IsNewFile(100, "file1.txt")
	require.False(t, isNew, "File with same timestamp and same name should not be new")

	err = w.Commit(200, "file3.txt", time.Hour, nil)
	require.NoError(t, err)

	isNew = w.IsNewFile(200, "file4.txt")
	require.True(t, isNew, "File with existing timestamp but different name should be new")

	isNew = w.IsNewFile(200, "file3.txt")
	require.False(t, isNew, "File with same timestamp and same name should not be new")
}

//---------------------------------------------------------------------------------------------------------------------
// GENERATED

func TestNewLWindow(t *testing.T) {
	w := NewLWindow(0)
	require.NotNil(t, w)
	require.NotNil(t, w.committed)
	require.True(t, w.committed.IsEmpty())
}

func TestIsNewFile_FileAlreadyCommitted(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	isNew := w.IsNewFile(100, "file1.txt")
	require.False(t, isNew, "File already committed should not be new")
}

func TestIsNewFile_EmptyWindow(t *testing.T) {
	w := NewLWindow(0)
	isNew := w.IsNewFile(100, "file1.txt")
	require.True(t, isNew, "File in empty window should be new")
}

func TestIsNewFile_FileTSGreaterThanRightTs(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	err = w.Commit(200, "file2.txt", time.Hour, nil)
	require.NoError(t, err)
	isNew := w.IsNewFile(300, "file3.txt")
	require.True(t, isNew, "File with TS > rightTs should be new")
}

func TestIsNewFile_FileTSLessThanLeftTs(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(200, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	err = w.Commit(300, "file2.txt", time.Hour, nil)
	require.NoError(t, err)
	isNew := w.IsNewFile(100, "file3.txt")
	require.False(t, isNew, "File with TS < leftTs should not be new")
}

func TestIsNewFile_FileTSInWindow(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	err = w.Commit(300, "file2.txt", time.Hour, nil)
	require.NoError(t, err)
	isNew := w.IsNewFile(200, "file3.txt")
	require.True(t, isNew, "File with TS in window should be new")
}

func TestIsNewFile_FileTSOnBoundaries(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	err = w.Commit(300, "file2.txt", time.Hour, nil)
	require.NoError(t, err)
	isNew := w.IsNewFile(100, "file3.txt")
	require.True(t, isNew, "File with TS == leftTs should be new")
	isNew = w.IsNewFile(300, "file4.txt")
	require.True(t, isNew, "File with TS == rightTs should be new")
}

func TestCommit_Success(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	require.True(t, w.committed.ContainsValue("file1.txt"))
	key, err := w.committed.GetKeyByValue("file1.txt")
	require.NoError(t, err)
	require.Equal(t, int64(100), key)
}

func TestCommit_DuplicateValue(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	err = w.Commit(200, "file1.txt", time.Hour, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")
}

func TestCommit_QueueToHandleNil(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	require.Equal(t, 1, w.committed.Size())
}

func TestCommit_QueueToHandleEmpty(t *testing.T) {
	w := NewLWindow(0)
	emptyQueue := ordered_multimap.NewOrderedMultimap()
	err := w.Commit(100, "file1.txt", time.Hour, emptyQueue)
	require.NoError(t, err)
	require.Equal(t, 1, w.committed.Size())
}

func TestCommit_CleanTail_BasicCase(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	err = w.Commit(200, "file2.txt", time.Hour, nil)
	require.NoError(t, err)
	err = w.Commit(300, "file3.txt", time.Hour, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(250, "queue1.txt")
	require.NoError(t, err)
	sumWindowTime := time.Hour
	err = w.Commit(400, "file4.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.Equal(t, 4, w.committed.Size())
}

func TestCommit_CleanTail_RightBorderGreaterThanSumWindow(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(100)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(1000, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(2000, "file2.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(3000, "file3.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(2500, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(4000, "file4.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.False(t, w.committed.Contains(1000))
	require.False(t, w.committed.Contains(2000))
	require.True(t, w.committed.Contains(3000))
	require.True(t, w.committed.Contains(4000))
}

func TestCommit_CleanTail_MinToHandleLessThanWindowBoundary(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(1000)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(1000, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(2000, "file2.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(3000, "file3.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(1500, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(4000, "file4.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.False(t, w.committed.Contains(1000))
	require.True(t, w.committed.Contains(2000))
	require.True(t, w.committed.Contains(3000))
	require.True(t, w.committed.Contains(4000))
}

func TestCommit_CleanTail_MinToHandleGreaterThanWindowBoundary(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(1000)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(1000, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(2000, "file2.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(3000, "file3.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(2500, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(4000, "file4.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.False(t, w.committed.Contains(1000))
	require.False(t, w.committed.Contains(2000))
	require.True(t, w.committed.Contains(3000))
	require.True(t, w.committed.Contains(4000))
}

func TestCommit_CleanTail_NegativeLeftBorder(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(10000)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(100, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(200, "file2.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(50, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(300, "file3.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.True(t, w.committed.Contains(100))
	require.True(t, w.committed.Contains(200))
	require.True(t, w.committed.Contains(300))
}

func TestCommit_CleanTail_AllFilesRemoved(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(100)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(100, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(200, "file2.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(5000, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(300, "file3.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.False(t, w.committed.Contains(100))
	require.True(t, w.committed.Contains(200))
	require.True(t, w.committed.Contains(300))
}

func TestCommit_CleanTail_NoFilesRemoved(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(1000)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(5000, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(6000, "file2.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(100, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(7000, "file3.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.True(t, w.committed.Contains(5000))
	require.True(t, w.committed.Contains(6000))
	require.True(t, w.committed.Contains(7000))
}

func TestCommit_CleanTail_MultipleValuesPerKey(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(1000)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(1000, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(1000, "file2.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(2000, "file3.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(1500, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(3000, "file4.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.False(t, w.committed.Contains(1000))
	require.True(t, w.committed.Contains(2000))
	require.True(t, w.committed.Contains(3000))
}

func TestCommit_CleanTail_EdgeCase_ZeroTimestamp(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(1000)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(0, "file0.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(1000, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(500, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(2000, "file2.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.False(t, w.committed.Contains(0))
	require.True(t, w.committed.Contains(1000))
	require.True(t, w.committed.Contains(2000))
}

func TestCommit_CleanTail_EdgeCase_NegativeMinToHandle(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(1000)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(1000, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(2000, "file2.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(-100, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(3000, "file3.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.True(t, w.committed.Contains(1000))
	require.True(t, w.committed.Contains(2000))
	require.True(t, w.committed.Contains(3000))
}

func TestIsNewFile_ErrorFromFirstPair(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	isNew := w.IsNewFile(200, "file2.txt")
	require.True(t, isNew)
	isNew = w.IsNewFile(150, "file3.txt")
	require.True(t, isNew)
}

func TestIsNewFile_ErrorFromLastPair(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	isNew := w.IsNewFile(200, "file2.txt")
	require.True(t, isNew)
}

func TestCommit_ErrorFromLastPair(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(50, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(200, "file2.txt", time.Hour, queue)
	require.NoError(t, err)
}

func TestCommit_ErrorFromFirstPair(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(50, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(200, "file2.txt", time.Hour, queue)
	require.NoError(t, err)
}

func TestCommit_CleanTail_LeftBorderExactlyZero(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(1000)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(1000, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(0, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(2000, "file2.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.True(t, w.committed.Contains(1000))
	require.True(t, w.committed.Contains(2000))
}

func TestCommit_CleanTail_LeftBorderPositive(t *testing.T) {
	w := NewLWindow(0)
	sumWindowNS := int64(500)
	sumWindowTime := time.Duration(sumWindowNS) * time.Nanosecond
	err := w.Commit(100, "file1.txt", sumWindowTime, nil)
	require.NoError(t, err)
	err = w.Commit(200, "file2.txt", sumWindowTime, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(150, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(1000, "file3.txt", sumWindowTime, queue)
	require.NoError(t, err)
	require.False(t, w.committed.Contains(100))
	require.True(t, w.committed.Contains(200))
	require.True(t, w.committed.Contains(1000))
}

func TestIsNewFile_ErrorFromFirstPairAfterEmptyCheck(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	isNew := w.IsNewFile(200, "file2.txt")
	require.True(t, isNew)
	isNew = w.IsNewFile(100, "file1.txt")
	require.False(t, isNew)
}

func TestCommit_ErrorFromLastPairAfterAdd(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(50, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(200, "file2.txt", time.Hour, queue)
	require.NoError(t, err)
	require.True(t, w.committed.Contains(100))
	require.True(t, w.committed.Contains(200))
}

func TestCommit_ErrorFromQueueFirstPair(t *testing.T) {
	w := NewLWindow(0)
	err := w.Commit(100, "file1.txt", time.Hour, nil)
	require.NoError(t, err)
	queue := ordered_multimap.NewOrderedMultimap()
	err = queue.Add(50, "queue1.txt")
	require.NoError(t, err)
	err = w.Commit(200, "file2.txt", time.Hour, queue)
	require.NoError(t, err)
}
