package gobwrapper

import (
	"encoding/gob"
	"reflect"
	"sort"

	"github.com/transferia/transferia/pkg/util/set"
)

var knownObjects = set.NewSyncSet[string]()

func RegisterName(name string, value any) {
	knownObjects.Add(name)
	gob.RegisterName(name, value)
}

func Register(value any) {
	rt := reflect.TypeOf(value)
	name := rt.String()
	knownObjects.Add(name)

	gob.Register(value)
}

func KnownGobRegisteredNames() []string {
	names := knownObjects.Slice()
	sort.Strings(names)
	return names
}
