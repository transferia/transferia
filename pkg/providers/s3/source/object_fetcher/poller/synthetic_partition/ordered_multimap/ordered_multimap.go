package ordered_multimap

import (
	"slices"
	"sort"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher/file"
)

type OrderedMultiMap struct {
	keys    []int64
	hashMap map[int64][]*file.File
	size    int
}

func (m *OrderedMultiMap) sortKeys() {
	sort.Slice(m.keys, func(i, j int) bool { return m.keys[i] < m.keys[j] })
}

func (m *OrderedMultiMap) Keys() []int64 {
	return m.keys
}

func (m *OrderedMultiMap) Values() [][]*file.File {
	result := make([][]*file.File, 0, len(m.hashMap))
	for _, v := range m.hashMap {
		result = append(result, v)
	}
	return result
}

func (m *OrderedMultiMap) Size() int {
	return len(m.keys)
}

func (m *OrderedMultiMap) AllSize() int {
	return m.size
}

func (m *OrderedMultiMap) Empty() bool {
	return len(m.keys) == 0
}

func (m *OrderedMultiMap) Reset() {
	m.keys = make([]int64, 0)
	m.hashMap = make(map[int64][]*file.File)
}

func (m *OrderedMultiMap) FirstKey() (int64, error) {
	if len(m.keys) == 0 {
		return 0, xerrors.New("len(m.keys) == 0")
	}
	return m.keys[0], nil
}

func (m *OrderedMultiMap) FirstPair() (int64, []*file.File, error) {
	if len(m.keys) == 0 {
		return 0, nil, xerrors.New("len(m.keys) == 0")
	}
	firstKey := m.keys[0]
	return firstKey, m.hashMap[firstKey], nil
}

func (m *OrderedMultiMap) LastPair() (int64, []*file.File, error) {
	if len(m.keys) == 0 {
		return 0, nil, xerrors.New("len(m.keys) == 0")
	}
	lastKey := m.keys[len(m.keys)-1]
	return lastKey, m.hashMap[lastKey], nil
}

func (m *OrderedMultiMap) Add(key int64, inFile *file.File) error {
	_, ok := m.hashMap[key]
	if !ok { // new key
		m.hashMap[key] = []*file.File{inFile}
		m.keys = append(m.keys, key)
		m.sortKeys()
	} else { // known key
		m.hashMap[key] = append(m.hashMap[key], inFile)
	}
	m.size++
	return nil
}

func (m *OrderedMultiMap) Del(key int64) error {
	v, ok := m.hashMap[key]
	if !ok { // key not found
		return xerrors.Errorf("key not found, key: %d", key)
	} else { // key found
		m.size -= len(v)
		delete(m.hashMap, key)
		m.keys = yslices.Filter(m.keys, func(el int64) bool { // rely it saves the order
			return el != key
		})
		return nil
	}
}

func (m *OrderedMultiMap) DelOne(key int64, fileName string) error {
	_, ok := m.hashMap[key]
	if !ok { // key not found
		return xerrors.Errorf("key not found, key: %d", key)
	}

	// key found
	if len(m.hashMap[key]) == 1 {
		// just Del
		if m.hashMap[key][0].FileName != fileName {
			return xerrors.Errorf("invariant broken, key: %d, fileName: %s", key, fileName)
		}
		return m.Del(key)
	} else {
		oldLen := len(m.hashMap[key])
		m.hashMap[key] = yslices.Filter(m.hashMap[key], func(in *file.File) bool {
			return in.FileName != fileName
		})
		newLen := len(m.hashMap[key])
		if newLen != oldLen-1 {
			return xerrors.Errorf("invariant broken, key: %d, oldLen: %d, newLen: %d", key, oldLen, newLen)
		}
		m.size--
		return nil
	}
}

func (m *OrderedMultiMap) Get(key int64) ([]*file.File, error) {
	_, ok := m.hashMap[key]
	if !ok {
		return nil, xerrors.Errorf("key not found, key: %d", key)
	} else {
		return m.hashMap[key], nil
	}
}

func (m *OrderedMultiMap) FindClosestKey(key int64) (int64, error) {
	if len(m.keys) == 0 {
		return int64(0), xerrors.New("len(m.keys) == 0")
	}
	index, found := slices.BinarySearch(m.keys, key)
	if found {
		return m.keys[index], nil
	}
	// not found
	if index == 0 {
		return key, nil
	}
	return m.keys[index-1], nil
}

func NewOrderedMultiMap() *OrderedMultiMap {
	return &OrderedMultiMap{
		keys:    make([]int64, 0),
		hashMap: make(map[int64][]*file.File),
		size:    0,
	}
}
