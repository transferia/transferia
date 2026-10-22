package postgres

import (
	"encoding/json"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
)

func TestHstoreToJSON(t *testing.T) {
	result0, err := HstoreToJSON("")
	require.NoError(t, err)
	require.Equal(t, "{}", result0)

	result1, err := HstoreToJSON("{}")
	require.NoError(t, err)
	require.Equal(t, "{}", result1)

	result2, err := HstoreToJSON(`{"a": "b"}`)
	require.NoError(t, err)
	require.Equal(t, `{"a": "b"}`, result2)

	result3, err := HstoreToJSON(`"a"=>"b"`)
	require.NoError(t, err)
	require.Equal(t, `{"a":"b"}`, result3)
}

func TestJSONToHstore(t *testing.T) {
	result0, err := JSONToHstore(`{"a":"b"}`)
	require.NoError(t, err)
	require.Equal(t, `"a"=>"b"`, result0)

	result1, err := JSONToHstore(`{"a":null}`)
	require.NoError(t, err)
	require.Equal(t, `"a"=>NULL`, result1)

	result2, err := JSONToHstore(`{"a":"b", "b":null}`)
	require.NoError(t, err)
	require.True(t, result2 == `"b"=>NULL, "a"=>"b"` || result2 == `"a"=>"b", "b"=>NULL`)
}

func TestJSONToHstoreRoundTrip(t *testing.T) {
	in := map[string]interface{}{
		"lf":    "Ivanov\nTel.89001234567",
		"crlf":  "a\r\nb",
		"tab":   "a\tb",
		"ff":    "a\fb",
		"k\nT":  "v",
		"quote": `say "hi"`,
		"slash": `a\b`,
		"null":  "NULL",
		"empty": "",
		"nil":   nil,
	}
	js, err := json.Marshal(in)
	require.NoError(t, err)
	hstore, err := JSONToHstore(string(js))
	require.NoError(t, err)
	out, err := HstoreToMap(hstore)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestEncodeHstoreTextUndefined(t *testing.T) {
	_, err := encodeHstoreText(&pgtype.Hstore{})
	require.Error(t, err)

	_, err = encodeHstoreText(&pgtype.Hstore{Map: map[string]pgtype.Text{"k": {}}, Status: pgtype.Present})
	require.Error(t, err)
}

func TestNestedHstoreRoundTrip(t *testing.T) {
	const hstoreOID = 16000
	ci := pgtype.NewConnInfo()
	ci.RegisterDataType(pgtype.DataType{Value: &pgtype.Hstore{}, Name: "hstore", OID: hstoreOID})
	want := map[string]pgtype.Text{"k\nT": {String: "a\tb", Status: pgtype.Present}}

	t.Run("array", func(t *testing.T) {
		src := &GenericArray{
			elements:   []pgtype.Value{&pgtype.Hstore{Map: want, Status: pgtype.Present}, &pgtype.Hstore{Status: pgtype.Null}},
			dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			status:     pgtype.Present,
			elementOID: hstoreOID,
		}
		bin, err := src.EncodeBinary(ci, nil)
		require.NoError(t, err)
		value, err := (&array{name: "_hstore", elementTypeOID: hstoreOID, logger: logger.Log}).NewValue(ci, nil)
		require.NoError(t, err)
		arr := value.(*GenericArray)
		require.NoError(t, arr.DecodeBinary(ci, bin))

		text, err := arr.EncodeText(ci, nil)
		require.NoError(t, err)
		decoded := &GenericArray{newElement: func() (pgtype.Value, error) { return new(pgtype.Hstore), nil }}
		require.NoError(t, decoded.DecodeText(ci, text))
		require.Len(t, decoded.elements, 2)
		require.Equal(t, want, decoded.elements[0].(*pgtype.Hstore).Map)
		require.Equal(t, pgtype.Null, decoded.elements[1].(*pgtype.Hstore).Status)
	})

	t.Run("composite", func(t *testing.T) {
		fields := []pgtype.CompositeTypeField{{Name: "h", OID: hstoreOID}}
		transcoders, err := makeValueTranscoders(ci, fields)
		require.NoError(t, err)
		composite, err := pgtype.NewCompositeTypeValues("t", fields, transcoders)
		require.NoError(t, err)
		require.NoError(t, composite.Set([]interface{}{want}))

		text, err := composite.EncodeText(ci, nil)
		require.NoError(t, err)
		decoded, err := pgtype.NewCompositeTypeValues("t", fields, []pgtype.ValueTranscoder{new(pgtype.Hstore)})
		require.NoError(t, err)
		require.NoError(t, decoded.DecodeText(ci, text))
		require.Equal(t, want, decoded.Get().(map[string]interface{})["h"])
	})
}

func TestHstoreValueEncodeTextEmpty(t *testing.T) {
	value := &hstoreValue{Hstore: pgtype.Hstore{Map: map[string]pgtype.Text{}, Status: pgtype.Present}}
	text, err := value.EncodeText(nil, nil)
	require.NoError(t, err)
	require.NotNil(t, text)
	require.Empty(t, text)
}
