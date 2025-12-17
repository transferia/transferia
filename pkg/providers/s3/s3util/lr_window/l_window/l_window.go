package l_window

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/ordered_multimap"
)

type LWindow struct {
	committed       *ordered_multimap.OrderedMultimap
	overlapDuration time.Duration
}

func (w *LWindow) IsNewFile(
	currFileNS int64,
	fileName string,
) bool {
	// file already committed
	if w.committed.ContainsValue(fileName) {
		return false
	}

	// if window 'committed' is empty - all files are new
	if w.committed.IsEmpty() {
		return true
	}

	// Контракт такой
	//
	// Если файл уже тут (committed) - return false
	// L_WINDOW дает нам окно таймштемпов [A...B]
	//   - если FILE_TS < B -- это NEW
	//   - если FILE_TS > A -- это OK
	//   - если FILE_TS входит в промежуток [A...B] - то поскольку файл тут неизвестен - он новый

	leftTs, _, err := w.committed.FirstPair()
	if err != nil {
		return true
	}
	rightTs, _, err := w.committed.LastPair()
	if err != nil {
		return true
	}

	if currFileNS > rightTs {
		return true
	} else if currFileNS < leftTs {
		return false
	} else {
		// ts into window
		return true
	}
}

func (w *LWindow) Commit(
	currFileNS int64,
	fileName string,
	listTimeDuration time.Duration,
	queueToHandle *ordered_multimap.OrderedMultimap,
) error {
	sumWindowDuration := w.overlapDuration + listTimeDuration

	// add
	err := w.committed.Add(currFileNS, fileName)
	if err != nil {
		return xerrors.Errorf("failed to add file '%s' to committed, err: %w", fileName, err)
	}

	// clean tail
	rightIncludeBorderNS, _, err := w.committed.LastPair()
	if err != nil {
		return xerrors.Errorf("failed to get last pair, err: %w", err)
	}

	lastIncludedBorderNS := rightIncludeBorderNS - sumWindowDuration.Nanoseconds()
	if queueToHandle != nil && !queueToHandle.IsEmpty() {
		minToHandleNS, _, err := queueToHandle.FirstPair()
		if err != nil {
			return xerrors.Errorf("failed to get first pair, err: %w", err)
		}
		lastIncludedBorderNS = min(minToHandleNS, rightIncludeBorderNS-sumWindowDuration.Nanoseconds())
	}

	if lastIncludedBorderNS < 0 {
		lastIncludedBorderNS = 0
	}

	w.committed.RemoveLessThan(lastIncludedBorderNS)
	return nil
}

func (w *LWindow) Serialize() ([]byte, error) {
	return w.committed.Serialize()
}

func (w *LWindow) Deserialize(in []byte) error {
	return w.committed.Deserialize(in)
}

func NewLWindow(overlapDuration time.Duration) *LWindow {
	return &LWindow{
		committed:       ordered_multimap.NewOrderedMultimap(),
		overlapDuration: overlapDuration,
	}
}
