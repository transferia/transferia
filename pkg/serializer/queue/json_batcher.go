package queue

import (
	"strings"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
)

func isJSONExtraElementViolatesConstraint(batchingSettings model.Batching, startIndex, currIndex int, lenElements []uint64, prevBodiesSize uint64) bool {
	numNewChangeItems := currIndex - startIndex + 1
	if batchingSettings.MaxMessageSize != 0 {
		newLinesNum := numNewChangeItems - 1
		if prevBodiesSize+uint64(newLinesNum)+lenElements[currIndex] > uint64(batchingSettings.MaxMessageSize) {
			return true
		}
	}

	if batchingSettings.MaxChangeItems != 0 {
		if numNewChangeItems > batchingSettings.MaxChangeItems {
			return true
		}
	}

	return false
}

func BatchJSON(batchingSettings model.Batching, in []abstract.ChangeItem) ([]SerializedMessage, error) {
	serializedElements := make([]string, len(in))
	lenElements := make([]uint64, len(in))
	for i := range in {
		buf, err := serializeQueueItemToJSON(&in[i])
		if err != nil {
			return nil, xerrors.Errorf("unable to serialize json, err: %w", err)
		}
		serializedElements[i] = string(buf)
		lenElements[i] = uint64(len(serializedElements[i]))
	}

	result := make([]SerializedMessage, 0, len(in))
	startIndex := 0
	sumBodiesSizeSize := uint64(0)
	for i := range in {
		if isJSONExtraElementViolatesConstraint(batchingSettings, startIndex, i, lenElements, sumBodiesSizeSize) {
			// dump prev
			numPrevChangeItems := i - startIndex
			if numPrevChangeItems == 0 {
				// one message violated constrain (it can be only message size) - dump it
				result = append(result, SerializedMessage{Key: nil, Value: []byte(serializedElements[startIndex])})
				startIndex = i + 1
				sumBodiesSizeSize = 0
			} else {
				result = append(result, SerializedMessage{Key: nil, Value: []byte(strings.Join(serializedElements[startIndex:i], "\n"))})
				startIndex = i
				sumBodiesSizeSize = lenElements[i]
			}
		} else {
			sumBodiesSizeSize += lenElements[i]
		}
	}
	if startIndex != len(in) {
		result = append(result, SerializedMessage{Key: nil, Value: []byte(strings.Join(serializedElements[startIndex:], "\n"))})
	}
	return result, nil
}
