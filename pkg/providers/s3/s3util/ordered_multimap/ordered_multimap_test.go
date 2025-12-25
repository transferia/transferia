package ordered_multimap

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOrderedMultimap(t *testing.T) {
	m := NewOrderedMultimap()
	require.NotNil(t, m)
	require.NotNil(t, m.data)
	require.NotNil(t, m.keys)
	require.Len(t, m.keys, 0)
	require.Len(t, m.data, 0)
}

func TestAdd(t *testing.T) {
	m := NewOrderedMultimap()

	// Test adding single value
	err := m.Add(1, "value1")
	require.NoError(t, err)
	require.True(t, m.Contains(1))
	values, err := m.Get(1)
	require.NoError(t, err)
	require.Len(t, values, 1)
	require.Equal(t, "value1", values[0])

	// Test adding multiple values to same key
	err = m.Add(1, "value2")
	require.NoError(t, err)
	values, err = m.Get(1)
	require.NoError(t, err)
	expected := []string{"value1", "value2"}
	require.Equal(t, expected, values)

	// Test adding duplicate value to same key should return error
	err = m.Add(1, "value1")
	require.Error(t, err)

	// Test adding different keys
	_ = m.Add(3, "value3")
	_ = m.Add(2, "value4")

	// Check sorted order
	keys := m.Keys()
	expectedKeys := []int64{1, 2, 3}
	require.Equal(t, expectedKeys, keys)
}

func TestAddSortedOrder(t *testing.T) {
	m := NewOrderedMultimap()

	// Add keys in random order with unique values
	keys := []int64{5, 1, 3, 2, 4}
	for i, key := range keys {
		err := m.Add(key, fmt.Sprintf("value%d", i))
		require.NoError(t, err)
	}

	// Check they are stored in sorted order
	actualKeys := m.Keys()
	expectedKeys := []int64{1, 2, 3, 4, 5}
	require.Equal(t, expectedKeys, actualKeys)
}

func TestGet(t *testing.T) {
	m := NewOrderedMultimap()

	// Test getting non-existent key
	_, err := m.Get(1)
	require.Error(t, err)

	// Test getting existing key
	_ = m.Add(1, "value1")
	_ = m.Add(1, "value2")
	values, err := m.Get(1)
	require.NoError(t, err)
	expected := []string{"value1", "value2"}
	require.Equal(t, expected, values)

	// Test that returned slice is a copy (modification doesn't affect original)
	values[0] = "modified"
	originalValues, err := m.Get(1)
	require.NoError(t, err)
	require.NotEqual(t, "modified", originalValues[0])
}

func TestContains(t *testing.T) {
	m := NewOrderedMultimap()

	// Test empty map
	require.False(t, m.Contains(1))

	// Test after adding
	_ = m.Add(1, "value")
	require.True(t, m.Contains(1))
	require.False(t, m.Contains(2))

	// Test after removing
	_ = m.Remove(1)
	require.False(t, m.Contains(1))
}

func TestContainsValue(t *testing.T) {
	m := NewOrderedMultimap()

	// Test non-existent value
	require.False(t, m.ContainsValue("value"))

	// Test existing values
	_ = m.Add(1, "value1")
	_ = m.Add(1, "value2")

	require.True(t, m.ContainsValue("value1"))
	require.True(t, m.ContainsValue("value2"))
	require.False(t, m.ContainsValue("value3"))
}

func TestRemove(t *testing.T) {
	m := NewOrderedMultimap()

	// Test removing from empty map
	err := m.Remove(1)
	require.Error(t, err)

	// Test removing non-existent key
	_ = m.Add(1, "value")
	err = m.Remove(2)
	require.Error(t, err)

	// Test removing existing key
	err = m.Remove(1)
	require.NoError(t, err)
	require.False(t, m.Contains(1))

	// Test that key is removed from sorted order
	_ = m.Add(1, "value1")
	_ = m.Add(3, "value3")
	_ = m.Add(2, "value2")
	err = m.Remove(2)
	require.NoError(t, err)

	keys := m.Keys()
	expected := []int64{1, 3}
	require.Equal(t, expected, keys)
}

func TestRemoveValue(t *testing.T) {
	m := NewOrderedMultimap()

	// Test removing non-existent value
	err := m.RemoveValue("value")
	require.Error(t, err)

	// Test removing non-existent value when map has data
	_ = m.Add(1, "value1")
	err = m.RemoveValue("value2")
	require.Error(t, err)

	// Test removing existing value
	_ = m.Add(1, "value2")
	err = m.RemoveValue("value1")
	require.NoError(t, err)

	values, err := m.Get(1)
	require.NoError(t, err)
	expected := []string{"value2"}
	require.Equal(t, expected, values)

	// Test removing last value removes the key
	err = m.RemoveValue("value2")
	require.NoError(t, err)
	require.False(t, m.Contains(1))
}

func TestSize(t *testing.T) {
	m := NewOrderedMultimap()

	// Test empty map
	require.Equal(t, 0, m.Size())

	// Test after adding keys
	_ = m.Add(1, "value1")
	require.Equal(t, 1, m.Size())

	_ = m.Add(1, "value2") // Same key, different value
	require.Equal(t, 1, m.Size())

	_ = m.Add(2, "value3") // Different key
	require.Equal(t, 2, m.Size())

	// Test after removing
	_ = m.Remove(1)
	require.Equal(t, 1, m.Size())
}

func TestTotalSize(t *testing.T) {
	m := NewOrderedMultimap()

	// Test empty map
	require.Equal(t, 0, m.TotalSize())

	// Test after adding values
	_ = m.Add(1, "value1")
	_ = m.Add(1, "value2")
	_ = m.Add(2, "value3")

	require.Equal(t, 3, m.TotalSize())

	// Test after removing value
	_ = m.RemoveValue("value1")
	require.Equal(t, 2, m.TotalSize())

	// Test after removing key
	_ = m.Remove(1)
	require.Equal(t, 1, m.TotalSize())
}

func TestClear(t *testing.T) {
	m := NewOrderedMultimap()

	// Add some data
	_ = m.Add(1, "value1")
	_ = m.Add(2, "value2")
	_ = m.Add(1, "value3")

	// Clear
	m.Clear()

	// Test everything is cleared
	require.Equal(t, 0, m.Size())
	require.Equal(t, 0, m.TotalSize())
	require.Len(t, m.Keys(), 0)
	require.Len(t, m.Values(), 0)
	require.False(t, m.Contains(1) || m.Contains(2))
}

func TestKeys(t *testing.T) {
	m := NewOrderedMultimap()

	// Test empty map
	keys := m.Keys()
	require.Len(t, keys, 0)

	// Test with data
	_ = m.Add(3, "value3")
	_ = m.Add(1, "value1")
	_ = m.Add(2, "value2")

	keys = m.Keys()
	expected := []int64{1, 2, 3}
	require.Equal(t, expected, keys)

	// Test that returned slice is a copy
	keys[0] = 999
	originalKeys := m.Keys()
	require.NotEqual(t, 999, originalKeys[0])
}

func TestValues(t *testing.T) {
	m := NewOrderedMultimap()

	// Test empty map
	values := m.Values()
	require.Len(t, values, 0)

	// Test with data (keys should be processed in sorted order)
	_ = m.Add(3, "value3a")
	_ = m.Add(1, "value1a")
	_ = m.Add(1, "value1b")
	_ = m.Add(2, "value2a")

	values = m.Values()
	// Values should be in order of sorted keys: 1, 2, 3
	expected := []string{"value1a", "value1b", "value2a", "value3a"}
	require.Equal(t, expected, values)
}

func TestIsEmpty(t *testing.T) {
	m := NewOrderedMultimap()

	// Test empty map
	require.True(t, m.IsEmpty())

	// Test after adding
	_ = m.Add(1, "value")
	require.False(t, m.IsEmpty())

	// Test after removing
	_ = m.Remove(1)
	require.True(t, m.IsEmpty())

	// Test after clear
	_ = m.Add(1, "value")
	m.Clear()
	require.True(t, m.IsEmpty())
}

func TestString(t *testing.T) {
	m := NewOrderedMultimap()

	// Test empty map
	str := m.String()
	expected := "OrderedMultimap{}"
	require.Equal(t, expected, str)

	// Test with data
	_ = m.Add(2, "value2")
	_ = m.Add(1, "value1a")
	_ = m.Add(1, "value1b")

	str = m.String()
	// Should be in sorted key order
	expectedPattern := "OrderedMultimap{1: [value1a, value1b], 2: [value2]}"
	require.Equal(t, expectedPattern, str)
}

func TestForEach(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(3, "value3")
	_ = m.Add(1, "value1a")
	_ = m.Add(1, "value1b")
	_ = m.Add(2, "value2")

	var results []string
	m.ForEach(func(key int64, value string) {
		results = append(results, value)
	})

	// Should be in sorted key order
	expected := []string{"value1a", "value1b", "value2", "value3"}
	require.Equal(t, expected, results)
}

func TestForEachKey(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(3, "value3")
	_ = m.Add(1, "value1a")
	_ = m.Add(1, "value1b")
	_ = m.Add(2, "value2")

	var keys []int64
	var allValues [][]string
	m.ForEachKey(func(key int64, values []string) {
		keys = append(keys, key)
		allValues = append(allValues, values)
	})

	expectedKeys := []int64{1, 2, 3}
	require.Equal(t, expectedKeys, keys)

	expectedValues := [][]string{
		{"value1a", "value1b"},
		{"value2"},
		{"value3"},
	}
	require.Equal(t, expectedValues, allValues)
}

func TestEdgeCases(t *testing.T) {
	m := NewOrderedMultimap()

	// Test with int64 max value
	maxKey := int64(9223372036854775807)
	_ = m.Add(maxKey, "max_value")
	require.True(t, m.Contains(maxKey))

	// Test with zero key
	_ = m.Add(0, "zero_value")
	require.True(t, m.Contains(0))

	// Test keys should be in correct order
	_ = m.Add(1, "one")
	keys := m.Keys()
	require.Equal(t, int64(0), keys[0])
	require.Equal(t, int64(1), keys[1])
	require.Equal(t, maxKey, keys[2])

	// Test empty string value
	_ = m.Add(100, "")
	require.True(t, m.ContainsValue(""))

	// Test duplicate values for same key (now should return error on second add)
	_ = m.Add(200, "duplicate")
	err := m.Add(200, "duplicate")
	require.Error(t, err)
	values, err := m.Get(200)
	require.NoError(t, err)
	require.Len(t, values, 1)
	require.Equal(t, "duplicate", values[0])
}

func TestAddDuplicateValueError(t *testing.T) {
	m := NewOrderedMultimap()

	// Test adding first value should succeed
	err := m.Add(1, "value1")
	require.NoError(t, err)

	// Test adding different value to same key should succeed
	err = m.Add(1, "value2")
	require.NoError(t, err)

	// Test adding duplicate value to same key should return error (globally unique)
	err = m.Add(1, "value1")
	require.Error(t, err)

	// Verify that the duplicate was not added
	values, err := m.Get(1)
	require.NoError(t, err)
	expected := []string{"value1", "value2"}
	require.Equal(t, expected, values)

	// Test adding same value to different key should also fail (globally unique)
	err = m.Add(2, "value1")
	require.Error(t, err)

	// Test adding different unique value to different key should succeed
	err = m.Add(2, "value3")
	require.NoError(t, err)
}

func TestConcurrentModificationSafety(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(1, "value1")
	_ = m.Add(2, "value2")

	// Test that modifying returned slices doesn't affect the map
	keys := m.Keys()
	originalLen := len(keys)
	_ = append(keys, 999) // This should not affect the map

	newKeys := m.Keys()
	require.Equal(t, originalLen, len(newKeys))

	values, err := m.Get(1)
	require.NoError(t, err)
	_ = append(values, "new_value") // This should not affect the map

	originalValues, err := m.Get(1)
	require.NoError(t, err)
	require.Len(t, originalValues, 1)
	require.Equal(t, "value1", originalValues[0])
}

func TestFindClosestKey(t *testing.T) {
	m := NewOrderedMultimap()

	// Test on empty map
	key, err := m.FindClosestKey(5)
	require.NoError(t, err)
	require.Equal(t, int64(5), key)

	// Add some keys: 10, 20, 30, 40, 50
	_ = m.Add(10, "ten")
	_ = m.Add(20, "twenty")
	_ = m.Add(30, "thirty")
	_ = m.Add(40, "forty")
	_ = m.Add(50, "fifty")

	// Test finding exact key
	key, err = m.FindClosestKey(30)
	require.NoError(t, err)
	require.Equal(t, int64(30), key)

	// Test finding key between existing keys (should return previous key)
	key, err = m.FindClosestKey(25)
	require.NoError(t, err)
	require.Equal(t, int64(20), key)

	// Test finding key between other keys
	key, err = m.FindClosestKey(35)
	require.NoError(t, err)
	require.Equal(t, int64(30), key)

	// Test finding key smaller than all keys (should return the requested key)
	key, err = m.FindClosestKey(5)
	require.NoError(t, err)
	require.Equal(t, int64(5), key)

	// Test finding key larger than all keys (should return last key)
	key, err = m.FindClosestKey(100)
	require.NoError(t, err)
	require.Equal(t, int64(50), key)

	// Test finding key just before first key
	key, err = m.FindClosestKey(9)
	require.NoError(t, err)
	require.Equal(t, int64(9), key)

	// Test finding key just after last key
	key, err = m.FindClosestKey(51)
	require.NoError(t, err)
	require.Equal(t, int64(50), key)
}

func TestGetKeyByValue(t *testing.T) {
	m := NewOrderedMultimap()

	// Test on empty map
	_, err := m.GetKeyByValue("nonexistent")
	require.Error(t, err)

	// Add some values
	_ = m.Add(10, "apple")
	_ = m.Add(20, "banana")
	_ = m.Add(30, "cherry")
	_ = m.Add(10, "date")

	// Test finding existing values
	key, err := m.GetKeyByValue("apple")
	require.NoError(t, err)
	require.Equal(t, int64(10), key)

	key, err = m.GetKeyByValue("banana")
	require.NoError(t, err)
	require.Equal(t, int64(20), key)

	key, err = m.GetKeyByValue("cherry")
	require.NoError(t, err)
	require.Equal(t, int64(30), key)

	key, err = m.GetKeyByValue("date")
	require.NoError(t, err)
	require.Equal(t, int64(10), key)

	// Test finding non-existent value
	_, err = m.GetKeyByValue("elderberry")
	require.Error(t, err)

	// Test after removing a value
	_ = m.RemoveValue("apple")
	_, err = m.GetKeyByValue("apple")
	require.Error(t, err)

	// Verify other values still exist
	key, err = m.GetKeyByValue("date")
	require.NoError(t, err)
	require.Equal(t, int64(10), key)
}

func TestUniqueValueConstraint(t *testing.T) {
	m := NewOrderedMultimap()

	// Test adding unique values to same key
	err := m.Add(1, "value1")
	require.NoError(t, err)

	err = m.Add(1, "value2")
	require.NoError(t, err)

	err = m.Add(1, "value3")
	require.NoError(t, err)

	// Test that duplicate value to same key fails
	err = m.Add(1, "value1")
	require.Error(t, err)

	// Test that same value to different key fails (globally unique)
	err = m.Add(2, "value1")
	require.Error(t, err)

	err = m.Add(2, "value2")
	require.Error(t, err)

	// Test adding unique value to different key succeeds
	err = m.Add(2, "value4")
	require.NoError(t, err)

	// Verify the state
	values1, err := m.Get(1)
	require.NoError(t, err)
	expectedValues1 := []string{"value1", "value2", "value3"}
	require.Equal(t, expectedValues1, values1)

	values2, err := m.Get(2)
	require.NoError(t, err)
	expectedValues2 := []string{"value4"}
	require.Equal(t, expectedValues2, values2)

	// Test that after removing a value, we can add it again
	_ = m.RemoveValue("value1")
	err = m.Add(2, "value1")
	require.NoError(t, err)

	// Verify value1 is now associated with key 2
	key, err := m.GetKeyByValue("value1")
	require.NoError(t, err)
	require.Equal(t, int64(2), key)
}

func TestGetKeyByValueAfterOperations(t *testing.T) {
	m := NewOrderedMultimap()

	_ = m.Add(1, "a")
	_ = m.Add(2, "b")
	_ = m.Add(3, "c")

	// Test GetKeyByValue after Remove
	_ = m.Remove(2)
	_, err := m.GetKeyByValue("b")
	require.Error(t, err)

	// Test GetKeyByValue after Clear
	m.Clear()
	_, err = m.GetKeyByValue("a")
	require.Error(t, err)
	_, err = m.GetKeyByValue("c")
	require.Error(t, err)

	// Add new values after clear
	_ = m.Add(10, "x")
	key, err := m.GetKeyByValue("x")
	require.NoError(t, err)
	require.Equal(t, int64(10), key)
}

func TestValueToKeyMapConsistency(t *testing.T) {
	m := NewOrderedMultimap()

	// Add values
	_ = m.Add(1, "v1")
	_ = m.Add(1, "v2")
	_ = m.Add(2, "v3")
	_ = m.Add(3, "v4")

	// Verify all values can be found
	for _, tc := range []struct {
		value       string
		expectedKey int64
	}{
		{"v1", 1},
		{"v2", 1},
		{"v3", 2},
		{"v4", 3},
	} {
		key, err := m.GetKeyByValue(tc.value)
		require.NoError(t, err)
		require.Equal(t, tc.expectedKey, key)
	}

	// Remove a key and verify its values are not findable
	_ = m.Remove(1)
	_, err := m.GetKeyByValue("v1")
	require.Error(t, err)
	_, err = m.GetKeyByValue("v2")
	require.Error(t, err)

	// But other values should still be findable
	key, err := m.GetKeyByValue("v3")
	require.NoError(t, err)
	require.Equal(t, int64(2), key)
}

func TestFindClosestKeyEdgeCases(t *testing.T) {
	m := NewOrderedMultimap()

	// Test with single key
	_ = m.Add(100, "hundred")

	key, err := m.FindClosestKey(100)
	require.NoError(t, err)
	require.Equal(t, int64(100), key)

	key, err = m.FindClosestKey(50)
	require.NoError(t, err)
	require.Equal(t, int64(50), key)

	key, err = m.FindClosestKey(150)
	require.NoError(t, err)
	require.Equal(t, int64(100), key)

	// Test with negative keys
	m.Clear()
	_ = m.Add(-50, "minus fifty")
	_ = m.Add(-30, "minus thirty")
	_ = m.Add(-10, "minus ten")
	_ = m.Add(10, "ten")
	_ = m.Add(30, "thirty")

	key, err = m.FindClosestKey(-20)
	require.NoError(t, err)
	require.Equal(t, int64(-30), key)

	key, err = m.FindClosestKey(0)
	require.NoError(t, err)
	require.Equal(t, int64(-10), key)

	key, err = m.FindClosestKey(-100)
	require.NoError(t, err)
	require.Equal(t, int64(-100), key)

	// Test with max and min int64 values
	m.Clear()
	maxInt64 := int64(9223372036854775807)
	minInt64 := int64(-9223372036854775808)

	_ = m.Add(minInt64, "min")
	_ = m.Add(0, "zero")
	_ = m.Add(maxInt64, "max")

	key, err = m.FindClosestKey(minInt64)
	require.NoError(t, err)
	require.Equal(t, minInt64, key)

	key, err = m.FindClosestKey(maxInt64)
	require.NoError(t, err)
	require.Equal(t, maxInt64, key)

	key, err = m.FindClosestKey(1000)
	require.NoError(t, err)
	require.Equal(t, int64(0), key)
}

func TestFirstPair(t *testing.T) {
	m := NewOrderedMultimap()

	// Test on empty map
	_, _, err := m.FirstPair()
	require.Error(t, err)

	// Test with single key
	_ = m.Add(10, "value10")
	key, values, err := m.FirstPair()
	require.NoError(t, err)
	require.Equal(t, int64(10), key)
	expectedValues := []string{"value10"}
	require.Equal(t, expectedValues, values)

	// Test with multiple keys (should return smallest key due to sorting)
	_ = m.Add(5, "value5")
	_ = m.Add(20, "value20")
	_ = m.Add(1, "value1")
	key, values, err = m.FirstPair()
	require.NoError(t, err)
	require.Equal(t, int64(1), key)
	expectedValues = []string{"value1"}
	require.Equal(t, expectedValues, values)

	// Test with multiple values for first key
	_ = m.Add(1, "value1b")
	_ = m.Add(1, "value1c")
	key, values, err = m.FirstPair()
	require.NoError(t, err)
	require.Equal(t, int64(1), key)
	expectedValues = []string{"value1", "value1b", "value1c"}
	require.Equal(t, expectedValues, values)

	// Test after removing first key
	_ = m.Remove(1)
	key, values, err = m.FirstPair()
	require.NoError(t, err)
	require.Equal(t, int64(5), key)
	expectedValues = []string{"value5"}
	require.Equal(t, expectedValues, values)
}

func TestLastPair(t *testing.T) {
	m := NewOrderedMultimap()

	// Test on empty map
	_, _, err := m.LastPair()
	require.Error(t, err)

	// Test with single key
	_ = m.Add(10, "value10")
	key, values, err := m.LastPair()
	require.NoError(t, err)
	require.Equal(t, int64(10), key)
	expectedValues := []string{"value10"}
	require.Equal(t, expectedValues, values)

	// Test with multiple keys (should return largest key due to sorting)
	_ = m.Add(5, "value5")
	_ = m.Add(20, "value20")
	_ = m.Add(1, "value1")
	key, values, err = m.LastPair()
	require.NoError(t, err)
	require.Equal(t, int64(20), key)
	expectedValues = []string{"value20"}
	require.Equal(t, expectedValues, values)

	// Test with multiple values for last key
	_ = m.Add(20, "value20b")
	_ = m.Add(20, "value20c")
	key, values, err = m.LastPair()
	require.NoError(t, err)
	require.Equal(t, int64(20), key)
	expectedValues = []string{"value20", "value20b", "value20c"}
	require.Equal(t, expectedValues, values)

	// Test after removing last key
	_ = m.Remove(20)
	key, values, err = m.LastPair()
	require.NoError(t, err)
	require.Equal(t, int64(10), key)
	expectedValues = []string{"value10"}
	require.Equal(t, expectedValues, values)
}

func TestFirstPairAndLastPairEdgeCases(t *testing.T) {
	m := NewOrderedMultimap()

	// Test when first and last are the same (single key)
	_ = m.Add(100, "single")
	firstKey, firstValues, err := m.FirstPair()
	require.NoError(t, err)
	lastKey, lastValues, err := m.LastPair()
	require.NoError(t, err)

	require.Equal(t, firstKey, lastKey)
	require.Equal(t, firstValues, lastValues)

	// Test with negative keys
	m.Clear()
	_ = m.Add(-50, "negative50")
	_ = m.Add(-10, "negative10")
	_ = m.Add(0, "zero")
	_ = m.Add(10, "positive10")

	firstKey, _, err = m.FirstPair()
	require.NoError(t, err)
	require.Equal(t, int64(-50), firstKey)

	lastKey, _, err = m.LastPair()
	require.NoError(t, err)
	require.Equal(t, int64(10), lastKey)

	// Test after Clear
	m.Clear()
	_, _, err = m.FirstPair()
	require.Error(t, err)
	_, _, err = m.LastPair()
	require.Error(t, err)

	// Test with extreme int64 values
	_ = m.Add(int64(-9223372036854775808), "min")
	_ = m.Add(int64(9223372036854775807), "max")
	_ = m.Add(0, "middle")

	firstKey, firstValues, err = m.FirstPair()
	require.NoError(t, err)
	require.Equal(t, int64(-9223372036854775808), firstKey)
	require.Equal(t, []string{"min"}, firstValues)

	lastKey, lastValues, err = m.LastPair()
	require.NoError(t, err)
	require.Equal(t, int64(9223372036854775807), lastKey)
	require.Equal(t, []string{"max"}, lastValues)
}

func TestFirstPairAndLastPairImmutability(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(1, "value1")
	_ = m.Add(2, "value2")
	_ = m.Add(3, "value3")

	// Get first pair
	_, firstValues, err := m.FirstPair()
	require.NoError(t, err)
	originalFirstLen := len(firstValues)

	// Modify returned slice
	firstValues = append(firstValues, "modified")

	// Get first pair again
	_, newFirstValues, err := m.FirstPair()
	require.NoError(t, err)
	require.Equal(t, originalFirstLen, len(newFirstValues))
	require.NotEqual(t, firstValues, newFirstValues)

	// Same test for LastPair
	_, lastValues, err := m.LastPair()
	require.NoError(t, err)
	originalLastLen := len(lastValues)

	lastValues = append(lastValues, "modified")

	_, newLastValues, err := m.LastPair()
	require.NoError(t, err)
	require.Equal(t, originalLastLen, len(newLastValues))
	require.NotEqual(t, lastValues, newLastValues)
}

func TestSerializeDeserialize(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(10, "ten0")
	_ = m.Add(10, "ten1")
	bytes, err := m.Serialize()
	require.NoError(t, err)
	require.Equal(t, `{"10":["ten0","ten1"]}`, string(bytes))

	m2 := NewOrderedMultimap()
	err = m2.Deserialize(bytes)
	require.NoError(t, err)
	require.Equal(t, 2, m2.TotalSize())
}

func TestDeserializeOnNonEmptyMultimap(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(1, "value1")

	jsonData := []byte(`{"2":["value2"]}`)
	err := m.Deserialize(jsonData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "multimap is not empty")
}

func TestDeserializeInvalidJSON(t *testing.T) {
	m := NewOrderedMultimap()
	invalidJSON := []byte(`invalid json`)

	err := m.Deserialize(invalidJSON)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to unmarshal map")
}

func TestDeserializeDuplicateValue(t *testing.T) {
	m := NewOrderedMultimap()
	jsonData := []byte(`{"1":["value1"],"2":["value1"]}`)

	err := m.Deserialize(jsonData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to add the value")
	require.Contains(t, err.Error(), "already exists")
}

func TestRemoveValueNotFoundInData(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(1, "value1")

	m.valueToKey["orphan"] = 1

	err := m.RemoveValue("orphan")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found in multimap")
}

func TestRemoveLessThan(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(10, "ten")
	_ = m.Add(20, "twenty")
	_ = m.Add(30, "thirty")
	_ = m.Add(40, "forty")
	_ = m.Add(50, "fifty")

	m.RemoveLessThan(30)

	require.Equal(t, 3, m.Size())
	keys := m.Keys()
	expectedKeys := []int64{30, 40, 50}
	require.Equal(t, expectedKeys, keys)

	require.False(t, m.Contains(10))
	require.False(t, m.Contains(20))
	require.True(t, m.Contains(30))
	require.True(t, m.Contains(40))
	require.True(t, m.Contains(50))

	require.False(t, m.ContainsValue("ten"))
	require.False(t, m.ContainsValue("twenty"))
	require.True(t, m.ContainsValue("thirty"))
	require.True(t, m.ContainsValue("forty"))
	require.True(t, m.ContainsValue("fifty"))
}

func TestRemoveLessThanWithMultipleValues(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(10, "ten_a")
	_ = m.Add(10, "ten_b")
	_ = m.Add(20, "twenty_a")
	_ = m.Add(20, "twenty_b")
	_ = m.Add(30, "thirty")

	m.RemoveLessThan(30)

	require.Equal(t, 1, m.Size())
	require.True(t, m.Contains(30))
	require.False(t, m.Contains(10))
	require.False(t, m.Contains(20))

	require.False(t, m.ContainsValue("ten_a"))
	require.False(t, m.ContainsValue("ten_b"))
	require.False(t, m.ContainsValue("twenty_a"))
	require.False(t, m.ContainsValue("twenty_b"))
	require.True(t, m.ContainsValue("thirty"))
}

func TestRemoveLessThanEmptyMap(t *testing.T) {
	m := NewOrderedMultimap()

	m.RemoveLessThan(10)

	require.True(t, m.IsEmpty())
	require.Equal(t, 0, m.Size())
}

func TestRemoveLessThanNoKeysToRemove(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(30, "thirty")
	_ = m.Add(40, "forty")
	_ = m.Add(50, "fifty")

	originalSize := m.Size()
	m.RemoveLessThan(10)

	require.Equal(t, originalSize, m.Size())
	keys := m.Keys()
	expectedKeys := []int64{30, 40, 50}
	require.Equal(t, expectedKeys, keys)
}

func TestRemoveLessThanAllKeys(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(10, "ten")
	_ = m.Add(20, "twenty")
	_ = m.Add(30, "thirty")

	m.RemoveLessThan(100)

	require.True(t, m.IsEmpty())
	require.Equal(t, 0, m.Size())
	require.Equal(t, 0, m.TotalSize())
}

func TestRemoveLessThanExactKey(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(10, "ten")
	_ = m.Add(20, "twenty")
	_ = m.Add(30, "thirty")

	m.RemoveLessThan(20)

	require.Equal(t, 2, m.Size())
	keys := m.Keys()
	expectedKeys := []int64{20, 30}
	require.Equal(t, expectedKeys, keys)

	require.False(t, m.Contains(10))
	require.True(t, m.Contains(20))
	require.True(t, m.Contains(30))
}

func TestRemoveLessThanValueToKeyConsistency(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(10, "v1")
	_ = m.Add(20, "v2")
	_ = m.Add(30, "v3")

	m.RemoveLessThan(25)

	_, err := m.GetKeyByValue("v1")
	require.Error(t, err)

	_, err = m.GetKeyByValue("v2")
	require.Error(t, err)

	key, err := m.GetKeyByValue("v3")
	require.NoError(t, err)
	require.Equal(t, int64(30), key)
}

func TestRemoveLessThanWithNegativeKeys(t *testing.T) {
	m := NewOrderedMultimap()
	_ = m.Add(-50, "minus_fifty")
	_ = m.Add(-30, "minus_thirty")
	_ = m.Add(-10, "minus_ten")
	_ = m.Add(0, "zero")
	_ = m.Add(10, "ten")

	m.RemoveLessThan(0)

	require.Equal(t, 2, m.Size())
	keys := m.Keys()
	expectedKeys := []int64{0, 10}
	require.Equal(t, expectedKeys, keys)

	require.False(t, m.Contains(-50))
	require.False(t, m.Contains(-30))
	require.False(t, m.Contains(-10))
	require.True(t, m.Contains(0))
	require.True(t, m.Contains(10))
}

func TestMigrate(t *testing.T) {
	orderedMultimap := NewOrderedMultimap()
	err := orderedMultimap.Deserialize([]byte(`{"NS":1636990841931000000,"Files":["csv_sample_100MB_no_header.csv"]}`))
	require.NoError(t, err)
}

func TestKeysInRange_EmptyMap(t *testing.T) {
	m := NewOrderedMultimap()
	result := m.KeysInRange(0, 100)
	require.Empty(t, result, "Empty map should return empty slice")
}

func TestKeysInRange_SingleKey(t *testing.T) {
	m := NewOrderedMultimap()
	require.NoError(t, m.Add(100, "file1.txt"))

	// Key in range
	result := m.KeysInRange(100, 100)
	require.Equal(t, []int64{100}, result, "Should return key when it's in range")

	// Key before range
	result = m.KeysInRange(0, 50)
	require.Empty(t, result, "Should return empty when key is before range")

	// Key after range
	result = m.KeysInRange(200, 300)
	require.Empty(t, result, "Should return empty when key is after range")
}

func TestKeysInRange_MultipleKeys(t *testing.T) {
	m := NewOrderedMultimap()
	require.NoError(t, m.Add(100, "file1.txt"))
	require.NoError(t, m.Add(200, "file2.txt"))
	require.NoError(t, m.Add(300, "file3.txt"))
	require.NoError(t, m.Add(400, "file4.txt"))
	require.NoError(t, m.Add(500, "file5.txt"))

	// Range in the middle
	result := m.KeysInRange(150, 450)
	require.Equal(t, []int64{200, 300, 400}, result, "Should return keys in range [150, 450]")

	// Range with exact boundaries
	result = m.KeysInRange(200, 400)
	require.Equal(t, []int64{200, 300, 400}, result, "Should return keys in range [200, 400]")

	// Range including all keys
	result = m.KeysInRange(100, 500)
	require.Equal(t, []int64{100, 200, 300, 400, 500}, result, "Should return all keys")

	// Range from first key
	result = m.KeysInRange(100, 300)
	require.Equal(t, []int64{100, 200, 300}, result, "Should return keys from first key")

	// Range to last key
	result = m.KeysInRange(300, 500)
	require.Equal(t, []int64{300, 400, 500}, result, "Should return keys to last key")

	// Range before all keys
	result = m.KeysInRange(0, 50)
	require.Empty(t, result, "Should return empty when range is before all keys")

	// Range after all keys
	result = m.KeysInRange(600, 700)
	require.Empty(t, result, "Should return empty when range is after all keys")

	// Range between keys
	result = m.KeysInRange(250, 250)
	require.Empty(t, result, "Should return empty when range is between keys")

	// Range with single key
	result = m.KeysInRange(200, 200)
	require.Equal(t, []int64{200}, result, "Should return single key when range is single key")
}

func TestKeysInRange_EdgeCases(t *testing.T) {
	m := NewOrderedMultimap()
	require.NoError(t, m.Add(100, "file1.txt"))
	require.NoError(t, m.Add(200, "file2.txt"))
	require.NoError(t, m.Add(300, "file3.txt"))

	// Range starting before first key
	result := m.KeysInRange(50, 250)
	require.Equal(t, []int64{100, 200}, result, "Should return keys from first key to boundary")

	// Range ending after last key
	result = m.KeysInRange(250, 500)
	require.Equal(t, []int64{300}, result, "Should return keys from boundary to last key")

	// Range with l > r (invalid range)
	result = m.KeysInRange(300, 100)
	require.Empty(t, result, "Should return empty when left > right")

	// Range with negative values
	require.NoError(t, m.Add(-100, "file0.txt"))
	result = m.KeysInRange(-150, -50)
	require.Equal(t, []int64{-100}, result, "Should handle negative keys")
}

func TestKeysInRange_LargeRange(t *testing.T) {
	m := NewOrderedMultimap()
	// Add keys with gaps
	require.NoError(t, m.Add(100, "file1.txt"))
	require.NoError(t, m.Add(500, "file2.txt"))
	require.NoError(t, m.Add(1000, "file3.txt"))
	require.NoError(t, m.Add(2000, "file4.txt"))
	require.NoError(t, m.Add(5000, "file5.txt"))

	// Large range covering all keys
	result := m.KeysInRange(0, 10000)
	require.Equal(t, []int64{100, 500, 1000, 2000, 5000}, result, "Should return all keys in large range")

	// Range covering some keys
	result = m.KeysInRange(200, 1500)
	require.Equal(t, []int64{500, 1000}, result, "Should return keys in range with gaps")
}

func TestKeysInRange_WithMultipleValuesPerKey(t *testing.T) {
	m := NewOrderedMultimap()
	// Add multiple values to same keys
	require.NoError(t, m.Add(100, "file1.txt"))
	require.NoError(t, m.Add(100, "file1b.txt"))
	require.NoError(t, m.Add(200, "file2.txt"))
	require.NoError(t, m.Add(300, "file3.txt"))
	require.NoError(t, m.Add(300, "file3b.txt"))
	require.NoError(t, m.Add(300, "file3c.txt"))

	// Range should return keys, not values (each key appears only once)
	result := m.KeysInRange(100, 300)
	require.Equal(t, []int64{100, 200, 300}, result, "Should return unique keys in range, not values")
	require.Len(t, result, 3, "Should return 3 unique keys")
}
