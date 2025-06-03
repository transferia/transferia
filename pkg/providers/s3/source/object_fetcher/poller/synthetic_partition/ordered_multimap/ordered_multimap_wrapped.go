package ordered_multimap

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher/file"
)

type OrderedMultiMapWrapped struct {
	multiMap       *OrderedMultiMap
	fileNameToFile map[string]*file.File
}

func (m *OrderedMultiMapWrapped) Keys() []int64 {
	return m.multiMap.Keys()
}

func (m *OrderedMultiMapWrapped) Values() [][]*file.File {
	return m.multiMap.Values()
}

func (m *OrderedMultiMapWrapped) Size() int {
	return m.multiMap.Size()
}

func (m *OrderedMultiMapWrapped) AllSize() int {
	return m.multiMap.AllSize()
}

func (m *OrderedMultiMapWrapped) Empty() bool {
	return m.multiMap.Empty()
}

func (m *OrderedMultiMapWrapped) Reset() {
	m.multiMap.Reset()
	m.fileNameToFile = make(map[string]*file.File)
}

func (m *OrderedMultiMapWrapped) FirstKey() (int64, error) {
	return m.multiMap.FirstKey()
}

func (m *OrderedMultiMapWrapped) FirstPair() (int64, []*file.File, error) {
	return m.multiMap.FirstPair()
}

func (m *OrderedMultiMapWrapped) LastPair() (int64, []*file.File, error) {
	return m.multiMap.LastPair()
}

func (m *OrderedMultiMapWrapped) Add(key int64, inFile *file.File) error { // add filename ONLY if not present here
	_, ok := m.fileNameToFile[inFile.FileName]
	if ok {
		return xerrors.Errorf("file %s already exists", inFile.FileName)
	}
	m.fileNameToFile[inFile.FileName] = inFile
	return m.multiMap.Add(key, inFile)
}

func (m *OrderedMultiMapWrapped) Del(key int64) error {
	values, err := m.multiMap.Get(key)
	if err != nil {
		return xerrors.Errorf("failed to get values from multi map: %w", err)
	}
	for _, v := range values {
		delete(m.fileNameToFile, v.FileName)
	}
	return m.multiMap.Del(key)
}

func (m *OrderedMultiMapWrapped) DelOne(key int64, fileName string) error {
	_, ok := m.fileNameToFile[fileName]
	if !ok {
		return xerrors.Errorf("file %s not found", fileName)
	}

	err := m.multiMap.DelOne(key, fileName)
	if err != nil {
		return xerrors.Errorf("failed to delete file %s: %w", fileName, err)
	}
	delete(m.fileNameToFile, fileName)
	return nil
}

func (m *OrderedMultiMapWrapped) Get(key int64) ([]*file.File, error) {
	return m.multiMap.Get(key)
}

func (m *OrderedMultiMapWrapped) FindClosestKey(key int64) (int64, error) {
	return m.multiMap.FindClosestKey(key)
}

// additional

func (m *OrderedMultiMapWrapped) FileByFileName(in string) (*file.File, error) {
	outFile, ok := m.fileNameToFile[in]
	if !ok {
		return nil, xerrors.Errorf("file %s not found", in)
	}
	return outFile, nil
}

func NewOrderedMultiMapWrapped() *OrderedMultiMapWrapped {
	return &OrderedMultiMapWrapped{
		multiMap:       NewOrderedMultiMap(),
		fileNameToFile: make(map[string]*file.File),
	}
}
