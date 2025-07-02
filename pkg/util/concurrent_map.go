package util

import "sync"

type ConcurrentMap[K comparable, V any] struct {
	mutex sync.RWMutex
	mp    map[K]V
}

func NewConcurrentMap[K comparable, V any]() *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{
		mp: make(map[K]V),
	}
}

func (m *ConcurrentMap[K, V]) Get(key K) (V, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	v, ok := m.mp[key]
	return v, ok
}

func (m *ConcurrentMap[K, V]) Set(key K, value V) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mp[key] = value
}

func (m *ConcurrentMap[K, V]) Delete(key K) (V, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	v, ok := m.mp[key]
	if !ok {
		return v, false
	}
	delete(m.mp, key)
	return v, true
}

func (m *ConcurrentMap[K, V]) Len() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.mp)
}

func (m *ConcurrentMap[K, V]) ListKeys() []K {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	var res []K
	for k := range m.mp {
		res = append(res, k)
	}
	return res
}

// Clear clears the map and calls the function with the map
// If argument is nil, the map will be cleared without calling the function
func (m *ConcurrentMap[K, V]) Clear(f func(map[K]V)) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if f != nil {
		f(m.mp)
	}
	m.mp = make(map[K]V)
}
