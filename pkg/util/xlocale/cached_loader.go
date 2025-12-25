package xlocale

import (
	"sync"
	"time"
)

var (
	locationCache sync.Map
	cacheMutex    sync.Mutex
)

func Load(name string) (*time.Location, error) {
	if entry, ok := locationCache.Load(name); ok {
		cacheEntry := entry.(*time.Location)
		return cacheEntry, nil
	}

	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}

	cacheMutex.Lock()
	locationCache.Store(name, loc)
	cacheMutex.Unlock()

	return loc, nil
}
