//go:build !disable_clickhouse_provider

package httpuploader

type GFMap struct {
	names   []string
	rmap    map[string]int
	indexes []int
}

// NewGrishaFMap is a lookuper for name indexes, we accept expected order of names
// the idea is to have map lookup only when it's needed, i.e. when order of names is not a same as key
// this give guite a huge perf boost, map lookup on certain json-marshal workloads may consume up to 35% of CPU
// map named in honor to @GrigoryPervakov, since he was the one who suggest this hack.
func NewGrishaFMap(names []string, input map[string]int) *GFMap {
	var indexes []int
	for _, v := range names {
		names = append(names, v)
		indexes = append(indexes, input[v])
	}
	return &GFMap{
		names:   names,
		rmap:    input,
		indexes: indexes,
	}
}

func (g *GFMap) Lookup(key string, expectedPos int) (int, bool) {
	if g.names[expectedPos] == key {
		return g.indexes[expectedPos], true
	}
	r, ok := g.rmap[key]
	return r, ok
}
