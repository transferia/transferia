package model

import (
	"sort"

	"github.com/transferria/transferria/pkg/abstract"
)

var (
	knownSources      = map[abstract.ProviderType]func() Source{}
	knownDestinations = map[abstract.ProviderType]func() Destination{}
)

// RegisterSource will add new source factory for specific provider type
// this should be placed inside provider `init() func`
func RegisterSource(typ abstract.ProviderType, fac func() Source) {
	knownSources[typ] = fac
}

// RegisterDestination will add new destination factory for specific provider type
// this should be placed inside provide `init() func`
func RegisterDestination(typ abstract.ProviderType, fac func() Destination) {
	knownDestinations[typ] = fac
}

func SourceF(typ abstract.ProviderType) (func() Source, bool) {
	f, ok := knownSources[typ]
	return f, ok
}

func DestinationF(typ abstract.ProviderType) (func() Destination, bool) {
	f, ok := knownDestinations[typ]
	return f, ok
}

func KnownSources() []string {
	var keys []string
	for k := range knownSources {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	return keys
}

func KnownDestinations() []string {
	var keys []string
	for k := range knownDestinations {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	return keys
}
