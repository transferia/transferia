package l_window

import (
	"encoding/json"
	"math"
	"unicode"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/ordered_multimap"
	"golang.org/x/exp/maps"
)

type serde struct {
	ToHandle map[int64][]string
	Handled  map[int64][]string
}

func serialize(in *LWindow) []byte {
	windowL, windowR := in.window()

	var serdeObj *serde
	if windowR != math.MinInt64 {
		serdeObj = &serde{
			ToHandle: in.toHandle.MapByRange(windowL, windowR),
			Handled:  in.handled.MapByRange(windowL, windowR),
		}
	} else {
		serdeObj = &serde{
			ToHandle: make(map[int64][]string),
			Handled:  make(map[int64][]string),
		}
	}

	result, _ := json.Marshal(serdeObj)
	return result
}

func deserialize(in []byte) (*ordered_multimap.OrderedMultimap, *ordered_multimap.OrderedMultimap, error) { // toHandle, handled
	var rawMap map[string]any
	_ = json.Unmarshal(in, &rawMap)

	isInt := func(s string) bool {
		for _, c := range s {
			if !unicode.IsDigit(c) {
				return false
			}
		}
		return true
	}

	allKeysNumbers := func(keys []string) bool {
		for _, k := range keys {
			if !isInt(k) {
				return false
			}
		}
		return true
	}

	if _, ok := rawMap["NS"]; ok && len(rawMap) == 2 {
		// first version -- 'handled':
		// 	NS    int64    `json:"NS"`
		//  Files []string `json:"Files"`

		var state stateUtilStructForSerDeForMigration
		err := json.Unmarshal(in, &state)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to deserialize json (2), err: %w", err)
		}
		handled := ordered_multimap.NewOrderedMultimap()
		for _, file := range state.Files {
			err = handled.Add(state.NS, file)
			if err != nil {
				return nil, nil, xerrors.Errorf("failed to deserialize json (2), err: %w", err)
			}
		}
		return ordered_multimap.NewOrderedMultimap(), handled, nil
	} else if allKeysNumbers(maps.Keys(rawMap)) {
		// second version -- 'handled':
		//  map[int64][]string

		extractOnlyNumbericKeys := func(in []byte) []byte {
			var rawMap2 map[string]any
			_ = json.Unmarshal(in, &rawMap2)
			tmp := map[string]any{}
			for k, v := range rawMap2 {
				if isInt(k) {
					tmp[k] = v
				}
			}
			tmp2, _ := json.Marshal(tmp)
			return tmp2
		}

		var state map[int64][]string
		err := json.Unmarshal(extractOnlyNumbericKeys(in), &state)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to deserialize json (2), err: %w", err)
		}
		handled, err := ordered_multimap.NewOrderedMultimapFromMap(state)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to deserialize json (2), err: %w", err)
		}
		return ordered_multimap.NewOrderedMultimap(), handled, nil
	} else {
		extractNeededKeys := func(in []byte) []byte {
			var rawMap2 map[string]any
			_ = json.Unmarshal(in, &rawMap2)
			tmp := map[string]any{}
			tmp["ToHandle"] = rawMap2["ToHandle"]
			tmp["Handled"] = rawMap2["Handled"]
			tmp2, _ := json.Marshal(tmp)
			return tmp2
		}

		var serdeObj serde
		err := json.Unmarshal(extractNeededKeys(in), &serdeObj)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to deserialize json (2), err: %w", err)
		}
		tohandle, err := ordered_multimap.NewOrderedMultimapFromMap(serdeObj.ToHandle)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to deserialize json (2), err: %w", err)
		}
		handled, err := ordered_multimap.NewOrderedMultimapFromMap(serdeObj.Handled)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to deserialize json (2), err: %w", err)
		}
		return tohandle, handled, nil
	}
}

type stateUtilStructForSerDeForMigration struct {
	NS    int64    `json:"NS"`
	Files []string `json:"Files"`
}
