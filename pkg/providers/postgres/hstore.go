package postgres

import (
	"encoding/json"

	"github.com/jackc/pgtype"
	pgtypev5 "github.com/jackc/pgx/v5/pgtype"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// encodeHstoreText quotes every key and value via pgx v5: pgtype v4 leaves "a\nb" bare, and Postgres rejects it.
func encodeHstoreText(src *pgtype.Hstore) ([]byte, error) {
	switch src.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, xerrors.New("hstore status is undefined")
	}
	hstore := make(pgtypev5.Hstore, len(src.Map))
	for k, v := range src.Map {
		switch v.Status {
		case pgtype.Present:
			hstore[k] = &v.String
		case pgtype.Null:
			hstore[k] = nil
		default:
			return nil, xerrors.Errorf("hstore value status of key %q is undefined", k)
		}
	}
	return pgtypev5.HstoreCodec{}.PlanEncode(nil, 0, pgtypev5.TextFormatCode, hstore).Encode(hstore, nil)
}

// hstoreValue makes arrays and composites encode nested hstore through encodeHstoreText.
type hstoreValue struct {
	pgtype.Hstore
}

var _ pgtype.ValueTranscoder = (*hstoreValue)(nil)

func (h *hstoreValue) EncodeText(_ *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	text, err := encodeHstoreText(&h.Hstore)
	if err != nil {
		return nil, xerrors.Errorf("unable to encode nested hstore: %w", err)
	}
	if text == nil || buf == nil {
		return text, nil
	}
	return append(buf, text...), nil
}

// newNestedValue creates an array element or a composite field value of the given type.
func newNestedValue(value pgtype.Value) pgtype.Value {
	if _, ok := value.(*pgtype.Hstore); ok {
		return new(hstoreValue)
	}
	return pgtype.NewValue(value)
}

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
	result, err := encodeHstoreText(&hstore)
	if err != nil {
		return "", xerrors.Errorf("unable to encode hstore: %w", err)
	}
	return string(result), nil
}
