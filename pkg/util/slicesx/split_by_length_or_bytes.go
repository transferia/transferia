package slicesx

import "math"

func SplitByBytesOrLength[T any](
	in []T,
	sizeFunc func(T) uint64,
	currentBytes uint64,
	currentLength uint64,
	maxBytes uint64,
	maxLength uint64,
) ([][]T, uint64, uint64) {

	if maxBytes == 0 && maxLength == 0 {
		return [][]T{in}, currentBytes, currentLength
	}
	if maxBytes == 0 {
		maxBytes = math.MaxUint64
	}
	if maxLength == 0 {
		maxLength = math.MaxUint64
	}

	var parts [][]T
	leftBound := 0
	for i := range in {
		currentBytes += sizeFunc(in[i])
		currentLength++

		if currentBytes >= maxBytes || currentLength >= maxLength {
			parts = append(parts, in[leftBound:i+1])
			currentBytes = 0
			currentLength = 0
			leftBound = i + 1
		}
	}

	if leftBound != len(in) {
		parts = append(parts, in[leftBound:])
	}

	return parts, currentBytes, currentLength
}
