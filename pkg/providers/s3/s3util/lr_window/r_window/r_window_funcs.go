package r_window

import (
	"encoding/json"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/ordered_multimap"
)

func buildRightWindow(in []file.File, listTime time.Duration, overlapWindowTimeSize time.Duration) (*ordered_multimap.OrderedMultimap, error) {
	sumWindowTimeSize := listTime + overlapWindowTimeSize

	inMap := ordered_multimap.NewOrderedMultimap()
	for _, object := range in {
		err := inMap.Add(object.LastModifiedNS, object.FileName)
		if err != nil {
			return nil, xerrors.Errorf("failed to add element into inMap in right window, lastModifiedNS:%d, fileName:%s, err: %w", object.LastModifiedNS, object.FileName, err)
		}
	}

	allKeys := inMap.Keys()
	if len(allKeys) == 0 {
		return inMap, nil
	}

	borderKey := allKeys[len(allKeys)-1]

	filteredKeys := allKeys
	filteredKeys = yslices.Filter(filteredKeys, func(elTimeNS int64) bool {
		return (elTimeNS <= borderKey) && (elTimeNS > borderKey-sumWindowTimeSize.Nanoseconds())
	})

	result := ordered_multimap.NewOrderedMultimap()
	for _, currKey := range filteredKeys {
		vales, err := inMap.Get(currKey)
		if err != nil {
			return nil, xerrors.Errorf("failed to get multiple objects, err: %w", err)
		}
		for _, currValue := range vales {
			err := result.Add(currKey, currValue)
			if err != nil {
				return nil, xerrors.Errorf("failed to add multiple objects, err: %w", err)
			}
		}
	}

	return result, nil
}

func deserializeWithMigration(in []byte) (*ordered_multimap.OrderedMultimap, error) {
	// not need to migrate
	stateForSerDe := stateUtilStructForSerDe{
		RWindow: make(map[int64][]string),
	}

	err := json.Unmarshal(in, &stateForSerDe)
	if err != nil {
		return nil, xerrors.Errorf("unable to unmarshal serialized state, err: %w", err)
	}

	result := ordered_multimap.NewOrderedMultimap()
	for rKey := range stateForSerDe.RWindow {
		for _, rVal := range stateForSerDe.RWindow[rKey] {
			err = result.Add(rKey, rVal)
			if err != nil {
				return nil, xerrors.Errorf("failed to add the value for key %d, err: %w", rKey, err)
			}
		}
	}
	return result, nil
}

type stateUtilStructForSerDe struct {
	RWindow map[int64][]string
}
