package changeitem

import (
	"sync"

	"github.com/transferia/transferia/pkg/util/set"
)

var (
	systemTables   = set.New[string]()
	systemTablesMu = &sync.RWMutex{}
)

func SystemTables() []string {
	systemTablesMu.RLock()
	defer systemTablesMu.RUnlock()
	return systemTables.Slice()
}

func RegisterSystemTables(tableNames ...string) {
	systemTablesMu.Lock()
	defer systemTablesMu.Unlock()
	systemTables.Add(tableNames...)
}

func IsSystemTable(in string) bool {
	systemTablesMu.RLock()
	defer systemTablesMu.RUnlock()
	return systemTables.Contains(in)
}
