package queue

import (
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func isNativeExtraElementViolatesConstraint(batchingSettings model.Batching, startIndex, currIndex int, lenElements []uint64, prevBodiesSize uint64) bool {
	numNewChangeItems := currIndex - startIndex + 1

	if batchingSettings.MaxMessageSize != 0 {
		commasNum := numNewChangeItems - 1
		if prevBodiesSize+uint64(commasNum)+lenElements[currIndex]+2 > uint64(batchingSettings.MaxMessageSize) {
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

func BatchNative(batchingSettings model.Batching, in []abstract.ChangeItem) []SerializedMessage {
	serializedElements := make([]string, len(in))
	lenElements := make([]uint64, len(in))
	for i := range in {
		serializedElements[i] = in[i].ToJSONString()
		lenElements[i] = uint64(len(serializedElements[i]))
	}

	result := make([]SerializedMessage, 0, len(in))
	startIndex := 0
	sumBodiesSizeSize := uint64(0)
	for i := range in {
		if isNativeExtraElementViolatesConstraint(batchingSettings, startIndex, i, lenElements, sumBodiesSizeSize) {
			// dump prev
			numPrevChangeItems := i - startIndex
			if numPrevChangeItems == 0 {
				// one message violated constrain (it can be only message size) - dump it
				// if needed further - we can introduce strict policy and return error here
				result = append(result, SerializedMessage{Key: nil, Value: []byte("[" + serializedElements[startIndex] + "]")})
				startIndex = i + 1
				sumBodiesSizeSize = 0
			} else {
				result = append(result, SerializedMessage{Key: nil, Value: []byte("[" + strings.Join(serializedElements[startIndex:i], ",") + "]")})
				startIndex = i
				sumBodiesSizeSize = lenElements[i]
			}
		} else {
			sumBodiesSizeSize += lenElements[i]
		}
	}
	if startIndex != len(in) {
		result = append(result, SerializedMessage{Key: nil, Value: []byte("[" + strings.Join(serializedElements[startIndex:], ",") + "]")})
	}
	return result
}
