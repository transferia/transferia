package maplock

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	mathutil "github.com/transferria/transferria/pkg/util/math"
)

const MaxRetry = 100000

func TestLockSuccess(t *testing.T) {
	m := NewMapMutex()

	if !m.TryLock("123") {
		t.Error("fail to get lock")
	}
	m.Unlock("123")
}

func TestLockFail(t *testing.T) {
	// fail fast
	m := NewCustomizedMapMutex(1, 1, 1, 2, 0.1)

	c := make(chan bool)
	finish := make(chan bool)

	num := 5
	success := make([]int, num)

	for i := 0; i < num; i++ {
		go func(i int) {
			if m.TryLock("123") {
				<-c // block here
				success[i] = 1
				m.Unlock("123")
			}
			finish <- true
		}(i)
	}

	// most goroutines fail to get the lock
	for i := 0; i < num-1; i++ {
		<-finish
	}

	sum := 0
	for _, s := range success {
		sum += s
	}

	if sum != 0 {
		t.Error("some other goroutine got the lock")
	}

	// finish the success one
	c <- true
	// wait
	<-finish
	for _, s := range success {
		sum += s
	}
	if sum != 1 {
		t.Error("no goroutine got the lock")
	}
}

func TestLockIndivisually(t *testing.T) {
	m := NewMapMutex()

	if !m.TryLock(123) || !m.TryLock(456) {
		t.Error("different locks affect each other")
	}
}

func BenchmarkMutex1000_100_20_20(b *testing.B)        { lockByOneMutex(1000, 100, 20, 20) }
func BenchmarkMapWithMutex1000_100_20_20(b *testing.B) { lockByMapWithMutex(1000, 100, 20, 20) }
func BenchmarkMapMutex1000_100_20_20(b *testing.B)     { lockByMapMutex(1000, 100, 20, 20) }

// less key, more conflict for map key
func BenchmarkMutex1000_20_20_20(b *testing.B)        { lockByOneMutex(1000, 20, 20, 20) }
func BenchmarkMapWithMutex1000_20_20_20(b *testing.B) { lockByMapWithMutex(1000, 20, 20, 20) }
func BenchmarkMapMutex1000_20_20_20(b *testing.B)     { lockByMapMutex(1000, 20, 20, 20) }

// less key, more goroutine, more conflict for map key
func BenchmarkMutex1000_20_40_20(b *testing.B)        { lockByOneMutex(1000, 20, 40, 20) }
func BenchmarkMapWithMutex1000_20_40_20(b *testing.B) { lockByMapWithMutex(1000, 20, 40, 20) }
func BenchmarkMapMutex1000_20_40_20(b *testing.B)     { lockByMapMutex(1000, 20, 40, 20) }

// even we want to use map to avoid unnecessary lock
// if case of only 2 entries, a lot of locking occurs
func BenchmarkMutex1000_2_40_20(b *testing.B)        { lockByOneMutex(1000, 2, 40, 20) }
func BenchmarkMapWithMutex1000_2_40_20(b *testing.B) { lockByMapWithMutex(1000, 2, 40, 20) }
func BenchmarkMapMutex1000_2_40_20(b *testing.B)     { lockByMapMutex(1000, 2, 40, 20) }

// longer time per job, more conflict for map key
func BenchmarkMutex1000_20_40_60(b *testing.B)        { lockByOneMutex(1000, 20, 40, 60) }
func BenchmarkMapWithMutex1000_20_40_60(b *testing.B) { lockByMapWithMutex(1000, 20, 40, 60) }
func BenchmarkMapMutex1000_20_40_60(b *testing.B)     { lockByMapMutex(1000, 20, 40, 60) }

// much more actions
func BenchmarkMutex10000_20_40_20(b *testing.B)        { lockByOneMutex(10000, 20, 40, 20) }
func BenchmarkMapWithMutex10000_20_40_20(b *testing.B) { lockByMapWithMutex(10000, 20, 40, 20) }
func BenchmarkMapMutex10000_20_40_20(b *testing.B)     { lockByMapMutex(10000, 20, 40, 20) }

// load should be larger than 0
func splitLoad(load, buckets int) []int {
	result := make([]int, buckets)
	avg := load / buckets
	remain := load % buckets

	// split
	for i := range result {
		result[i] = avg
		if remain > 0 {
			result[i]++
			remain--
		}
	}

	// randomize
	for i := 0; i < buckets; i += 2 {
		if i+1 < buckets {
			r := rand.Intn(mathutil.Min(result[i], result[i+1]))
			if rand.Intn(r+1)%2 == 0 {
				result[i] -= r
				result[i+1] += r
			} else {
				result[i] += r
				result[i+1] -= r
			}
		}
	}

	return result
}

func lockByOneMutex(actionCount, keyCount, goroutineNum, averageTime int) {
	sharedSlice := make([]int, keyCount)
	var m sync.Mutex

	loads := splitLoad(actionCount, goroutineNum)
	var wg sync.WaitGroup
	wg.Add(goroutineNum)
	success := make([]int, goroutineNum)
	for i, load := range loads {
		go func(i, load int) {
			success[i] = runWithOneMutex(load, keyCount, averageTime,
				sharedSlice, &m)
			wg.Done()
		}(i, load)
	}

	wg.Wait()
	sum := 0
	for _, s := range success {
		sum += s
	}
	fmt.Println("one mutex: ", actionCount, keyCount, goroutineNum, averageTime, "sum is: ", sum)
}

func lockByMapWithMutex(actionCount, keyCount, goroutineNum, averageTime int) {
	sharedSlice := make([]int, keyCount)
	locks := make(map[int]bool)
	var m sync.Mutex

	loads := splitLoad(actionCount, goroutineNum)
	var wg sync.WaitGroup
	wg.Add(goroutineNum)
	success := make([]int, goroutineNum)
	for i, load := range loads {
		go func(i, load int) {
			success[i] = runWithMapWithMutex(load, keyCount, averageTime,
				sharedSlice, &m, locks)
			wg.Done()
		}(i, load)
	}

	wg.Wait()
	sum := 0
	for _, s := range success {
		sum += s
	}
	fmt.Println("map with mutex: ", actionCount, keyCount, goroutineNum, averageTime, "sum is: ", sum)
}

func lockByMapMutex(actionCount, keyCount, goroutineNum, averageTime int) {
	sharedSlice := make([]int, keyCount)
	m := NewMapMutex()

	loads := splitLoad(actionCount, goroutineNum)
	var wg sync.WaitGroup
	wg.Add(goroutineNum)
	success := make([]int, goroutineNum)
	for i, load := range loads {
		go func(i, load int) {
			success[i] = runWithMapMutex(load, keyCount, averageTime,
				sharedSlice, m)
			wg.Done()
		}(i, load)
	}

	wg.Wait()
	sum := 0
	for _, s := range success {
		sum += s
	}
	fmt.Println("map mutex: ", actionCount, keyCount, goroutineNum, averageTime, "sum is: ", sum)
}

func runWithOneMutex(iterateNum, keyCount, averageTime int, sharedSlice []int,
	m *sync.Mutex) int {
	success := 0
	for ; iterateNum > 0; iterateNum-- {
		m.Lock()

		idx := rand.Intn(keyCount)
		doTheJob(averageTime, idx, sharedSlice)
		success++

		m.Unlock()
	}

	return success
}

func runWithMapWithMutex(iterateNum, keyCount, averageTime int,
	sharedSlice []int, m *sync.Mutex, locks map[int]bool) int {
	success := 0
	for ; iterateNum > 0; iterateNum-- {
		idx := rand.Intn(keyCount)
		goon := false
		for i := 0; i < MaxRetry; i++ {
			m.Lock()
			if locks[idx] { // if locked
				m.Unlock()
				time.Sleep(time.Duration(rand.Intn(100)*(i/100+1)) * time.Nanosecond)
			} else { // if unlock, lockit
				locks[idx] = true
				m.Unlock()
				goon = true
				break
			}
		}

		if !goon {
			continue // failed to get lock, go on for next iteration
		}
		doTheJob(averageTime, idx, sharedSlice)
		success++

		m.Lock()
		delete(locks, idx)
		m.Unlock()
	}
	return success
}

func runWithMapMutex(iterateNum, keyCount, averageTime int,
	sharedSlice []int, m *Mutex) int {
	success := 0
	for ; iterateNum > 0; iterateNum-- {
		idx := rand.Intn(keyCount)
		// fail to get lock
		if !m.TryLock(idx) {
			continue
		}

		doTheJob(averageTime, idx, sharedSlice)
		success++

		m.Unlock(idx)
	}
	return success
}

func doTheJob(averageTime, idx int, sharedSlice []int) {
	// do real job, just sleep some time and set a value
	miliSec := rand.Intn(averageTime * 2)
	time.Sleep(time.Duration(miliSec) * time.Millisecond)
	sharedSlice[idx] = miliSec
}
