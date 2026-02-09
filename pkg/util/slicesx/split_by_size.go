package slicesx

import (
	"errors"
	"fmt"
)

// OversizePolicy defines the behavior when a single element's size exceeds the limit.
type OversizePolicy int

const (
	// OversizePolicyAllow allows oversized elements to be placed in their own single-element chunk.
	OversizePolicyAllow OversizePolicy = iota
	// OversizePolicyError causes SplitBySize to return an error when an oversized element is encountered.
	OversizePolicyError
)

var (
	ErrOversizedElement = errors.New("element size exceeds split limit")
	ErrNonPositiveLimit = errors.New("splitByBytesVal must be positive")
	ErrNilSizeFunc      = errors.New("sizeFunc must not be nil")
	ErrInvalidPolicy    = errors.New("invalid oversize policy")
)

// SplitBySize splits a slice into chunks where each chunk's total size (determined by sizeFunc)
// is less or equal than splitByBytesVal. The oversizePolicy controls what happens when a single
// element's size is > splitByBytesVal.
func SplitBySize[T any](in []T, splitByBytesVal int, sizeFunc func(T) int, policy OversizePolicy) ([][]T, error) {
	if splitByBytesVal <= 0 {
		return nil, ErrNonPositiveLimit
	}

	if sizeFunc == nil {
		return nil, ErrNilSizeFunc
	}

	if len(in) == 0 {
		return nil, nil
	}

	var result [][]T
	var current []T
	currentSize := 0

	for i, item := range in {
		itemSize := sizeFunc(item)

		if itemSize < 0 {
			return nil, fmt.Errorf("sizeFunc returned negative size (%d) for element at index %d", itemSize, i)
		}

		if itemSize > splitByBytesVal {
			switch policy {
			case OversizePolicyError:
				return nil, fmt.Errorf("%w: element at index %d has size %d, limit is %d", ErrOversizedElement, i, itemSize, splitByBytesVal)
			case OversizePolicyAllow:
				// Flush current chunk if non-empty, then put oversized element in its own chunk.
				if len(current) > 0 {
					result = append(result, current)
					current = nil
					currentSize = 0
				}
				result = append(result, []T{item})
				continue
			default:
				return nil, ErrInvalidPolicy
			}
		}

		// If adding this item would exceed the limit, flush current chunk first.
		if len(current) > 0 && currentSize+itemSize > splitByBytesVal {
			result = append(result, current)
			current = nil
			currentSize = 0
		}

		current = append(current, item)
		currentSize += itemSize
	}

	if len(current) > 0 {
		result = append(result, current)
	}

	return result, nil
}
