//go:build !disable_mongo_provider

package mongo

import (
	"encoding/json"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.mongodb.org/mongo-driver/bson"
)

// DValue struct is used as document in change items in order to:
//  1. Provide jsonSerializable interface in our type system
//  2. Bring back legacy behaviour for typesystem < 7
type DValue struct {
	bson.D
	isHomo            bool // can be removed, if fallback knew it is homogeneous fallback
	preventJSONRepack bool // remove when transfers will be gone: https://github.com/transferia/transferia/review/3604892/details#comment--5003594
}

func MakeDValue(val bson.D, isHomo, preventJSONRepack bool) DValue {
	return DValue{
		D:                 val,
		isHomo:            isHomo,
		preventJSONRepack: preventJSONRepack,
	}
}

func (d DValue) MarshalJSON() ([]byte, error) {
	return bson.MarshalExtJSON(d.D, false, true)
}

func (d DValue) RepackValue() (interface{}, error) {
	if d.isHomo {
		return d.D, nil
	}
	if d.preventJSONRepack {
		valMarshalled, err := bson.Marshal(d.D)
		if err != nil {
			return nil, xerrors.Errorf("cannot marshal ordered BSON: %w", err)
		}
		var unordered bson.M
		err = bson.Unmarshal(valMarshalled, &unordered)
		if err != nil {
			return nil, xerrors.Errorf("cannot unmarshal unordered BSON: %w", err)
		}
		return unordered, nil
	}

	r, err := bson.MarshalExtJSON(d.D, false, true)
	if err != nil {
		return nil, xerrors.Errorf("unable to bson marshal: %w", err)
	}
	var repacked interface{}
	if err := json.Unmarshal(r, &repacked); err != nil {
		return nil, xerrors.Errorf("unable to repack bson: %w", err)
	}
	return repacked, nil
}

type DExtension struct {
	bson.D
	cache    bson.M
	modified bool
}

func DExt(root bson.D) *DExtension {
	res := DExtension{
		D:        make(bson.D, len(root)),
		cache:    bson.M{},
		modified: true,
	}
	copy(res.D, root)
	return &res
}

func (d *DExtension) SetKey(key string, val interface{}) {
	d.modified = true
	for i, entity := range d.D {
		if entity.Key == key {
			d.D[i].Value = val
			return
		}
	}
	d.D = append(d.D, bson.E{Key: key, Value: val})
}

func (d *DExtension) RawValue() bson.D {
	res := bson.D{}
	for _, e := range d.D {
		if e.Key != "_id" {
			res = append(res, e)
		}
	}
	return res
}

func (d *DExtension) Value(isHomo, preventJSONRepack bool) DValue {
	return MakeDValue(d.RawValue(), isHomo, preventJSONRepack)
}

func (d *DExtension) Map() bson.M {
	if d.modified {
		d.cache = d.D.Map()
		d.modified = false
	}
	return d.cache
}

func getKeyD(doc bson.D, key string) (any, bool) {
	for _, item := range doc {
		if item.Key == key {
			return item.Value, true
		}
	}
	return nil, false
}

func getKeyM(doc bson.M, key string) (any, bool) {
	val, ok := doc[key]
	return val, ok
}

func GetValueByPath(doc any, path string) (any, bool) {
	pathTokens := strings.Split(path, ".")
	val := doc
	for _, key := range pathTokens {
		var elem any
		ok := false
		switch v := val.(type) {
		case bson.M:
			elem, ok = getKeyM(v, key)
		case bson.D:
			elem, ok = getKeyD(v, key)
		default:
			return nil, false
		}
		if !ok {
			return nil, false
		}
		val = elem
	}
	return val, true
}

func SetValueByPath(doc any, path string, val any, emptyBsonContainerFactory func() any) (any, error) {
	if doc == nil {
		doc = emptyBsonContainerFactory()
	}
	if !isBsonContainer(doc) {
		return nil, xerrors.Errorf("unexpected value type %T, bson.M or bson.D are expectd", doc)
	}

	pathTokens := strings.Split(path, ".")
	lastIdx := len(pathTokens) - 1
	var result, parent, nextContainer any
	container := doc
	for i, key := range pathTokens {
		if i == lastIdx {
			switch r := container.(type) {
			case bson.M:
				setKeyM(r, key, val)
			case bson.D:
				container = setKeyD(r, key, val)
			}
		} else {
			var ok bool
			nextContainer, ok = GetValueByPath(container, key)
			if !ok || !isBsonContainer(nextContainer) {
				nextContainer = emptyBsonContainerFactory()
				var err error
				if container, err = SetValueByPath(container, key, nextContainer, emptyBsonContainerFactory); err != nil {
					return nil, xerrors.Errorf("cannot set middle key %v: %w", key, err)
				}
			}
		}
		if i == 0 {
			result = container
		} else {
			if _, err := SetValueByPath(parent, pathTokens[i-1], container, emptyBsonContainerFactory); err != nil {
				return nil, xerrors.Errorf("cannot update value for %v: %w", pathTokens[i-1], err)
			}
		}
		parent = container
		container = nextContainer
	}
	return result, nil
}

func isBsonContainer(val any) bool {
	switch val.(type) {
	case bson.M, bson.D:
		return true
	default:
		return false
	}
}

func setKeyM(doc bson.M, key string, val any) {
	doc[key] = val
}

func setKeyD(doc bson.D, key string, val any) bson.D {
	for i, e := range doc {
		if e.Key == key {
			doc[i].Value = val
			return doc
		}
	}
	return append(doc, bson.E{Key: key, Value: val})
}
