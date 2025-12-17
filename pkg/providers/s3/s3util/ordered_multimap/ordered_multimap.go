package ordered_multimap

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type OrderedMultimap struct {
	data       map[int64][]string
	keys       []int64
	valueToKey map[string]int64
}

func NewOrderedMultimap() *OrderedMultimap {
	return &OrderedMultimap{
		data:       make(map[int64][]string),
		keys:       make([]int64, 0),
		valueToKey: make(map[string]int64),
	}
}

func (m *OrderedMultimap) insertKeySorted(key int64) {
	pos := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= key
	})
	m.keys = append(m.keys, 0)
	copy(m.keys[pos+1:], m.keys[pos:])
	m.keys[pos] = key
}

func (m *OrderedMultimap) Add(key int64, value string) error {
	if existingKey, exists := m.valueToKey[value]; exists {
		return fmt.Errorf("value '%s' already exists in the multimap (associated with key %d)", value, existingKey)
	}

	if _, exists := m.data[key]; !exists {
		m.insertKeySorted(key)
		m.data[key] = make([]string, 0)
	}

	m.data[key] = append(m.data[key], value)
	m.valueToKey[value] = key
	return nil
}

func (m *OrderedMultimap) Get(key int64) ([]string, error) {
	values, exists := m.data[key]
	if !exists {
		return nil, fmt.Errorf("key %d not found in multimap", key)
	}
	result := make([]string, len(values))
	copy(result, values)
	return result, nil
}

func (m *OrderedMultimap) Contains(key int64) bool {
	_, exists := m.data[key]
	return exists
}

func (m *OrderedMultimap) ContainsValue(value string) bool {
	_, exists := m.valueToKey[value]
	return exists
}

func (m *OrderedMultimap) GetKeyByValue(value string) (int64, error) {
	key, exists := m.valueToKey[value]
	if !exists {
		return 0, fmt.Errorf("value '%s' not found in multimap", value)
	}
	return key, nil
}

func (m *OrderedMultimap) Remove(key int64) error {
	values, exists := m.data[key]
	if !exists {
		return fmt.Errorf("key %d not found in multimap", key)
	}

	// Remove all values from reverse map
	for _, value := range values {
		delete(m.valueToKey, value)
	}

	delete(m.data, key)
	pos := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= key
	})
	if pos < len(m.keys) && m.keys[pos] == key {
		m.keys = append(m.keys[:pos], m.keys[pos+1:]...)
	}
	return nil
}

func (m *OrderedMultimap) RemoveValue(value string) error {
	key, exists := m.valueToKey[value]
	if !exists {
		return fmt.Errorf("value '%s' not found in multimap", value)
	}

	values := m.data[key]
	for i, v := range values {
		if v == value {
			m.data[key] = append(values[:i], values[i+1:]...)
			delete(m.valueToKey, value)
			if len(m.data[key]) == 0 {
				_ = m.Remove(key)
			}
			return nil
		}
	}
	return fmt.Errorf("value '%s' not found in multimap", value)
}

func (m *OrderedMultimap) RemoveLessThan(key int64) {
	if len(m.keys) == 0 {
		return
	}

	pos := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= key
	})

	if pos == 0 {
		return
	}

	for i := 0; i < pos; i++ {
		k := m.keys[i]
		values := m.data[k]
		for _, value := range values {
			delete(m.valueToKey, value)
		}
		delete(m.data, k)
	}

	m.keys = m.keys[pos:]
}

func (m *OrderedMultimap) Size() int {
	return len(m.keys)
}

func (m *OrderedMultimap) TotalSize() int {
	total := 0
	for _, values := range m.data {
		total += len(values)
	}
	return total
}

func (m *OrderedMultimap) Clear() {
	m.data = make(map[int64][]string)
	m.keys = make([]int64, 0)
	m.valueToKey = make(map[string]int64)
}

func (m *OrderedMultimap) Keys() []int64 {
	result := make([]int64, len(m.keys))
	copy(result, m.keys)
	return result
}

func (m *OrderedMultimap) Values() []string {
	var result []string
	for _, key := range m.keys {
		result = append(result, m.data[key]...)
	}
	return result
}

func (m *OrderedMultimap) IsEmpty() bool {
	return len(m.keys) == 0
}

func (m *OrderedMultimap) String() string {
	if m.IsEmpty() {
		return "OrderedMultimap{}"
	}
	var parts []string
	for _, key := range m.keys {
		values := m.data[key]
		valuesStr := "[" + strings.Join(values, ", ") + "]"
		parts = append(parts, fmt.Sprintf("%d: %s", key, valuesStr))
	}
	return "OrderedMultimap{" + strings.Join(parts, ", ") + "}"
}

func (m *OrderedMultimap) ForEach(fn func(key int64, value string)) {
	for _, key := range m.keys {
		for _, value := range m.data[key] {
			fn(key, value)
		}
	}
}

func (m *OrderedMultimap) ForEachKey(fn func(key int64, values []string)) {
	for _, key := range m.keys {
		values := make([]string, len(m.data[key]))
		copy(values, m.data[key])
		fn(key, values)
	}
}

func (m *OrderedMultimap) FindClosestKey(key int64) (int64, error) {
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

func (m *OrderedMultimap) FirstPair() (int64, []string, error) {
	if len(m.keys) == 0 {
		return 0, nil, fmt.Errorf("multimap is empty")
	}
	firstKey := m.keys[0]
	firstValues := m.data[firstKey]
	return firstKey, firstValues, nil
}

func (m *OrderedMultimap) LastPair() (int64, []string, error) {
	if len(m.keys) == 0 {
		return 0, nil, fmt.Errorf("multimap is empty")
	}
	lastKey := m.keys[len(m.keys)-1]
	lastValues := m.data[lastKey]
	return lastKey, lastValues, nil
}

func (m *OrderedMultimap) Serialize() ([]byte, error) {
	return json.Marshal(m.data)
}

func (m *OrderedMultimap) Deserialize(in []byte) error {
	if !m.IsEmpty() {
		return fmt.Errorf("multimap is not empty")
	}

	stateForSerDe := make(map[int64][]string)
	var err error

	if isOldState(in) {
		stateForSerDe, err = migrate(in)
		if err != nil {
			return fmt.Errorf("migrate old state failed, err: %w", err)
		}
	} else {
		err := json.Unmarshal(in, &stateForSerDe)
		if err != nil {
			return fmt.Errorf("unable to unmarshal map, json:%s, err:%s", in, err.Error())
		}
	}

	for rKey := range stateForSerDe {
		for _, rVal := range stateForSerDe[rKey] {
			err = m.Add(rKey, rVal)
			if err != nil {
				return fmt.Errorf("failed to add the value for key %d, err: %s", rKey, err.Error())
			}
		}
	}

	return nil
}

//---

func migrate(in []byte) (map[int64][]string, error) {
	var state stateUtilStructForSerDeForMigration
	err := json.Unmarshal(in, &state)
	if err != nil {
		return nil, xerrors.Errorf("failed to deserialize json (2), err: %w", err)
	}
	return map[int64][]string{
		state.NS: state.Files,
	}, nil
}

type stateUtilStructForSerDeForMigration struct {
	NS    int64    `json:"NS"`
	Files []string `json:"Files"`
}

func isOldState(in []byte) bool {
	var rawMap map[string]any
	err := json.Unmarshal(in, &rawMap)
	if err != nil {
		return false
	}

	if _, ok := rawMap["NS"]; ok && len(rawMap) == 2 {
		return true
	} else {
		return false
	}
}
