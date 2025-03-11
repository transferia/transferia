package util

import "github.com/transferia/transferia/library/go/core/xerrors"

// XDArray Multidimensional array with arbitrary item type
type XDArray struct {
	Data []interface{}
}

func set(data []interface{}, d int, index []int, value interface{}) error {
	if d < len(index)-1 {
		i := index[d]
		item, ok := data[i].([]interface{})
		if !ok {
			return xerrors.New("invalid item")
		}
		return set(item, d+1, index, value)
	} else {
		data[index[d]] = value
		return nil
	}
}

func (xda *XDArray) Set(index []int, value interface{}) error {
	if len(index) == 0 {
		return xerrors.Errorf("invalid index: %v", index)
	}
	return set(xda.Data, 0, index, value)
}

func checkSize(size []int) error {
	if len(size) == 0 {
		return xerrors.New("size is empty")
	}
	for i, x := range size {
		if x < 0 {
			return xerrors.Errorf("invalid dimension: size[%v] = %v", i, x)
		}
	}
	return nil
}

func resize(data []interface{}, d int, size []int) {
	if d < len(size) {
		for i := 0; i < len(data); i++ {
			item := make([]interface{}, size[d])
			data[i] = item
			resize(item, d+1, size)
		}
	}
}

func EmptyXDArray() *XDArray {
	return &XDArray{
		Data: make([]interface{}, 0),
	}
}

func NewXDArray(size []int) (*XDArray, error) {
	if err := checkSize(size); err != nil {
		return nil, xerrors.Errorf("invalid size: %w", err)
	}
	xda := new(XDArray)
	xda.Data = make([]interface{}, size[0])
	resize(xda.Data, 1, size)
	return xda, nil
}

func FullSize(size []int) []int {
	fullSize := make([]int, len(size))
	copy(fullSize, size)
	fullSize[len(size)-1] = 1
	for i := len(size) - 1; i > 0; i-- {
		fullSize[i-1] = fullSize[i] * size[i]
	}
	return fullSize
}

func XDIndex(i int, fullSize []int) []int {
	index := make([]int, len(fullSize))
	for d, l := range fullSize {
		index[d] = i / l
		i = i % l
	}
	return index
}
