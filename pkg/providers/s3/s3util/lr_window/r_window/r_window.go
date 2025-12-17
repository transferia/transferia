package r_window

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/ordered_multimap"
)

type RWindow struct {
	sumTime time.Duration
	rWindow *ordered_multimap.OrderedMultimap
}

func (w *RWindow) Serialize() ([]byte, error) {
	stateForSerDe := stateUtilStructForSerDe{
		RWindow: make(map[int64][]string),
	}

	rKeys := w.rWindow.Keys()
	for _, rKey := range rKeys {
		lVals, err := w.rWindow.Get(rKey)
		if err != nil {
			return nil, xerrors.Errorf("failed to get the value for key %d, err: %w", rKey, err)
		}
		stateForSerDe.RWindow[rKey] = append(stateForSerDe.RWindow[rKey], lVals...)
	}

	result, _ := json.Marshal(stateForSerDe)
	return result, nil
}

// Контракт такой
// R_WINDOW дает нам окно таймштемпов [A...B]
//   - если FILE_TS > B -- это OUT-OF-WINDOW
//   - если FILE_TS < A -- это OK
//   - если FILE_TS входит в промежуток [A...B]
//     1) если FILE_TS есть в окне -- это наше
//     2) если FILE_TS нету в окне -- это OUT-OF-WINDOW
func (w *RWindow) IsOutOfRWindow(inFile *file.File) bool {
	if w.rWindow == nil || w.rWindow.IsEmpty() {
		return false
	}

	ts := inFile.LastModifiedNS
	leftTs, _, _ := w.rWindow.FirstPair()
	rightTs, _, _ := w.rWindow.LastPair()

	if ts > rightTs {
		return true
	} else if ts < leftTs {
		return false
	} else {
		// ts into window
		isKnownTS := w.rWindow.Contains(ts)
		if !isKnownTS {
			// unknown ts
			return true
		}

		knownFileNames, _ := w.rWindow.Get(ts)
		isKnownFileName := slices.Contains(knownFileNames, inFile.FileName)
		if isKnownFileName {
			return false
		} else {
			return true
		}
	}
}

func NewRWindowEmpty(inSumTime time.Duration) *RWindow {
	return &RWindow{
		sumTime: inSumTime,
		rWindow: ordered_multimap.NewOrderedMultimap(),
	}
}

func NewRWindowFromFiles(inSumTime time.Duration, listTime time.Duration, in []file.File) (*RWindow, error) {
	rWindow, err := buildRightWindow(in, listTime, inSumTime)
	if err != nil {
		return nil, xerrors.Errorf("failed to build the window: %w", err)
	}
	return &RWindow{
		sumTime: inSumTime,
		rWindow: rWindow,
	}, nil
}

func NewRWindowFromSerialized(inSumTime time.Duration, in []byte) (*RWindow, error) {
	rWindow, err := deserializeWithMigration(in)
	if err != nil {
		return nil, xerrors.Errorf("failed to deserialize the window: %w", err)
	}
	return &RWindow{
		sumTime: inSumTime,
		rWindow: rWindow,
	}, nil
}
