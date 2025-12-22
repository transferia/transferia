package l_window

import (
	"math"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/ordered_multimap"
)

// LWindow - Left Window.
// It's assumed to be moved from left to right by 'LastModifiedNS' timeline of files in s3.
//
// LWindow isn't thread-safe by design - LWindows assumed to be used singlt-threaded
// LWindow intentionally disigned to 'not to be transational-approach' - if errors occurred, object is invalid
// LWindow intentionally disigned without 'defensive' checks - assumed it will be used only from outside package:
//    * LWindow object will be created only via costructor, so fields can't be nil.
//    * Assumed filenames never can be empty string
//    * Assumed time can't be lower that zero

type LWindow struct {
	overlapDuration           time.Duration
	minAmountElementsInWindow int
	toHandle                  *ordered_multimap.OrderedMultimap // this container won't growth uncontrollable - bcs every 'Commit' removes element from here
	handled                   *ordered_multimap.OrderedMultimap // this container should be cleaned in 'Commit' - to prevent uncontrollable growth
}

func (w *LWindow) window() (int64, int64) {
	toHandleL, _ := w.toHandle.LR()
	handledL, handledR := w.handled.LR()

	return min(handledL, toHandleL), handledR
}

func (w *LWindow) isNewFile(
	currFileNS int64,
	fileName string,
) bool {
	// file already handled
	if w.handled.ContainsValue(fileName) {
		return false
	}

	// file in queue to handle
	if w.toHandle.ContainsValue(fileName) {
		return true
	}

	// if window 'handled' is empty - all files are new
	// it should happens only on activation (cold start)
	if w.handled.IsEmpty() {
		return true
	}

	// Contract here is:
	//
	// If file in 'handled' -- return false
	// If file in 'toHandle' -- return true
	// If on first run (cold start) - when 'handled' is empty - any file is new -- return true
	// ELSE
	// L_WINDOW.handled gives us window [LBorder...RBorder]
	//   - if FILE_TS < LBorder -- it's SOME TIME AGO ALREADY HANDLED
	//   - if FILE_TS >= LBorder -- it's NEW

	lBorder, _ := w.window()
	return currFileNS >= lBorder
}

func (w *LWindow) IsNewFile(
	currFileNS int64,
	fileName string,
) (bool, error) {
	isNewFile := w.isNewFile(currFileNS, fileName)
	if isNewFile {
		if !w.toHandle.ContainsValue(fileName) {
			err := w.toHandle.Add(currFileNS, fileName)
			if err != nil {
				return false, xerrors.Errorf("failed to add file '%s' to handle, err: %w", fileName, err)
			}
		}
	}
	return isNewFile, nil
}

func (w *LWindow) cleanHandledTail(sumWindowDurationNS int64) {
	// here are guaranteed 'w.handled' has at least one element - so, lBorderHandled and rBorderHandled can't be stubs
	lBorderHandled, rBorderHandled := w.handled.LR()
	handledWindowWidth := rBorderHandled - lBorderHandled

	if handledWindowWidth > sumWindowDurationNS {
		// need to clean

		leftStrongBorder, _ := w.toHandle.LR()
		leftSoftBorder := rBorderHandled - sumWindowDurationNS

		var newLeftBorder int64
		if leftStrongBorder == math.MaxInt64 {
			// we have only 'soft' border
			newLeftBorder = leftSoftBorder
		} else {
			// we have both: 'soft' and 'hard' border
			if leftSoftBorder > leftStrongBorder {
				newLeftBorder = leftStrongBorder
			} else {
				newLeftBorder = leftSoftBorder
			}
		}

		w.handled.RemoveLessThanWithMinElements(newLeftBorder, w.minAmountElementsInWindow) // assume 'newLeftBorder' never can be lower that 0
	}
}

func (w *LWindow) Commit(
	currFileNS int64,
	fileName string,
	listTimeDuration time.Duration,
) error {
	sumWindowDuration := w.overlapDuration + listTimeDuration

	// remove from 'toHandle'
	err := w.toHandle.RemoveValue(fileName)
	if err != nil {
		return xerrors.Errorf("Failed to remove file %s: %w", fileName, err)
	}

	// add
	err = w.handled.Add(currFileNS, fileName)
	if err != nil {
		return xerrors.Errorf("failed to add file '%s' to handled, err: %w", fileName, err)
	}

	w.cleanHandledTail(sumWindowDuration.Nanoseconds())
	return nil
}

func (w *LWindow) Serialize() []byte {
	return serialize(w)
}

func (w *LWindow) Deserialize(in []byte) error {
	toHandle, handled, err := deserialize(in)
	if err != nil {
		return xerrors.Errorf("Failed to deserialize: %w", err)
	}
	w.toHandle = toHandle
	w.handled = handled
	return nil
}

func NewLWindow(overlapDuration time.Duration, minAmountElementsInWindow int) *LWindow {
	return &LWindow{
		overlapDuration:           overlapDuration,
		minAmountElementsInWindow: minAmountElementsInWindow,
		toHandle:                  ordered_multimap.NewOrderedMultimap(),
		handled:                   ordered_multimap.NewOrderedMultimap(),
	}
}
