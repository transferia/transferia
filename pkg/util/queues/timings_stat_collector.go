package util

import (
	"strings"
	"sync"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type TimingsStatCollector struct {
	mutex sync.Mutex

	startFindWriter    map[abstract.TablePartID]time.Time
	durationFindWriter map[abstract.TablePartID]time.Duration

	startWrite    map[abstract.TablePartID]time.Time
	durationWrite map[abstract.TablePartID]time.Duration
}

func (t *TimingsStatCollector) Started(tableID abstract.TablePartID) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.startFindWriter[tableID] = time.Now()
}

func (t *TimingsStatCollector) FoundWriter(tableID abstract.TablePartID) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	var startTime time.Time
	var ok bool
	if startTime, ok = t.startFindWriter[tableID]; !ok {
		return // never should be
	}

	t.durationFindWriter[tableID] = time.Since(startTime)
	t.startWrite[tableID] = time.Now()
}

func (t *TimingsStatCollector) Finished(tableID abstract.TablePartID) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	var startTime time.Time
	var ok bool
	if startTime, ok = t.startWrite[tableID]; !ok {
		return // never should be
	}

	t.durationWrite[tableID] = time.Since(startTime)
}

func UnquotedFQTNWithPartID(tablePartID abstract.TablePartID) string {
	return strings.Join([]string{tablePartID.Namespace, tablePartID.Name, tablePartID.PartID}, ".")
}

func (t *TimingsStatCollector) GetResults() []log.Field {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	resultFindWriter := make(map[string]string)
	for k, v := range t.durationFindWriter {
		resultFindWriter[UnquotedFQTNWithPartID(k)] = v.String()
	}
	resultWrite := make(map[string]string)
	for k, v := range t.durationWrite {
		resultWrite[UnquotedFQTNWithPartID(k)] = v.String()
	}
	return []log.Field{log.Any("find_writer_stat", resultFindWriter), log.Any("write_stat", resultWrite)}
}

func NewTimingsStatCollector() *TimingsStatCollector {
	return &TimingsStatCollector{
		mutex:              sync.Mutex{},
		startFindWriter:    make(map[abstract.TablePartID]time.Time),
		durationFindWriter: make(map[abstract.TablePartID]time.Duration),
		startWrite:         make(map[abstract.TablePartID]time.Time),
		durationWrite:      make(map[abstract.TablePartID]time.Duration),
	}
}
