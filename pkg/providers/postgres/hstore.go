//go:build !disable_postgres_provider

package postgres

import (
	"encoding/json"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func HstoreToMap(colVal string) (map[string]interface{}, error) {
	hstore := pgtype.Hstore{}
	err := hstore.DecodeText(nil, []byte(colVal))
	if err != nil {
		return nil, xerrors.Errorf("hstore.DecodeText returned error, err: %w", err)
	}
	resultMap := make(map[string]interface{})
	for k, v := range hstore.Map {
		if v.Status == pgtype.Null {
			resultMap[k] = nil
		} else {
			resultMap[k] = v.String
		}
	}
	return resultMap, nil
}

func HstoreToJSON(colVal string) (string, error) {
	if colVal == "" {
		return "{}", nil
	} else if colVal[0] == '{' {
		return colVal, nil
	} else {
		resultMap, err := HstoreToMap(colVal)
		if err != nil {
			return "", xerrors.Errorf("unable to parse hstore, val: %s, err: %w", colVal, err)
		}
		result, err := json.Marshal(resultMap)
		if err != nil {
			return "", xerrors.Errorf("unable to marshal map, err: %w", err)
		}
		return string(result), nil
	}
}

func JSONToHstore(in string) (string, error) {
	var m map[string]interface{}
	err := json.Unmarshal([]byte(in), &m)
	if err != nil {
		return "", xerrors.Errorf("unable to unmarshal json: %w", err)
	}
	hstore := pgtype.Hstore{
		Map:    make(map[string]pgtype.Text),
		Status: pgtype.Present,
	}
	for k, v := range m {
		if v == nil {
			hstore.Map[k] = pgtype.Text{String: "", Status: pgtype.Null}
		} else {
			hstore.Map[k] = pgtype.Text{String: v.(string), Status: pgtype.Present}
		}
	}
	result, err := hstore.EncodeText(nil, make([]byte, 0))
	if err != nil {
		return "", xerrors.Errorf("unable to encode hstore: %w", err)
	}
	return string(result), nil
}
