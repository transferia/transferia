package postgres

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestParseTimestamp(t *testing.T) {
	got, err := extractTimestamp("2019-04-10 11:10:50.683494+03")
	require.NoError(t, err)
	if got != 1554883850683494000 {
		t.Errorf("Unable to parse got: %v", got)
	}
}

func TestParseTimestampNegativeTz(t *testing.T) {
	got, err := extractTimestamp("2019-04-10 11:10:50.683494-03")
	require.NoError(t, err)
	if got != 1554905450683494000 {
		t.Errorf("Unable to parse got: %v", got)
	}
}

func TestParseTimestampZeroTz(t *testing.T) {
	got, err := extractTimestamp("2019-04-10 11:10:50.683494+00")
	require.NoError(t, err)
	if got != 1554894650683494000 {
		t.Errorf("Unable to parse got: %v", got)
	}
}

func TestRestoreType(t *testing.T) {
	type TestPair struct {
		Val interface{} `json:"val"`
		Typ string      `json:"typ"`
	}
	goodJSON := `
	[
		{
			"val": true,
			"typ": "pg:boolean"
		},
		{
			"val": false,
			"typ": "pg:boolean"
		},
		{
			"val": 32767,
			"typ": "pg:smallint"
		},
		{
			"val": -32768,
			"typ": "pg:smallint"
		},
		{
			"val":  -2147483648,
			"typ": "pg:integer"
		},
		{
			"val": 2147483647,
			"typ": "pg:integer"
		},
		{
			"val": -922337203685470,
			"typ": "pg:bigint"
		},
		{
			"val": 922337203685470,
			"typ": "pg:bigint"
		},
		{
			"val": "$100.0",
			"typ": "pg:money"
		},
		{
			"val": "hello, friend",
			"typ": "pg:text"
		},
		{
			"val": 12.04,
			"typ": "pg:real"
		},
		{
			"val": "42.0",
			"typ": "pg:numeric"
		},
		{
			"val": "{\"one\":\"two\", \"array\": [1, 2, 3], \"obj\": {\"hello\" : [\"bye\", \"bye\", true, 0.1]}}",
			"typ": "pg:json"
		},
		{
			"val": "cafebabe",
			"typ": "pg:bytea"
		}
	]
	`
	var tests []TestPair
	bytes, _ := hex.DecodeString("cafebabe")
	results := []interface{}{
		true,
		false,
		int16(32767),
		int16(-32768),
		int32(-2147483648),
		int32(2147483647),
		int64(-922337203685470),
		int64(922337203685470),
		"$100.0",
		"hello, friend",
		json.Number("12.04"),
		json.Number("42.0"),
		map[string]interface{}{"one": "two", "array": []interface{}{json.Number("1"), json.Number("2"), json.Number("3")}, "obj": map[string]interface{}{"hello": []interface{}{"bye", "bye", true, json.Number("0.1")}}},
		bytes,
	}
	err := json.Unmarshal([]byte(goodJSON), &tests)
	require.NoError(t, err, "Bad test resource json")

	cp := defaultChangeProcessor()
	for i, v := range tests {
		t.Run(fmt.Sprintf("test %s", v.Typ), func(t *testing.T) {
			colSchema := newColSchemaForType(v.Typ)
			restored, err := cp.restoreType(v.Val, 0, colSchema)
			require.NoError(t, err)
			require.Exactly(t, restored, results[i], "Restore failed")
		})
	}
}

func TestRestoreTimestampDomain(t *testing.T) {
	const (
		timestamptzDomainOID pgtype.OID = 60001
		timestampDomainOID   pgtype.OID = 60002
	)

	tests := []struct {
		name         string
		oid          pgtype.OID
		registered   pgtype.Value
		homoResult   any
		value        string
		originalType string
	}{
		{
			name:         "timestamp with time zone domain",
			oid:          timestamptzDomainOID,
			registered:   new(pgtype.Timestamptz),
			homoResult:   pgtype.Timestamptz{},
			value:        "2026-07-06 03:00:00+03",
			originalType: "pg:classifier.date_t",
		},
		{
			name:         "timestamp without time zone domain",
			oid:          timestampDomainOID,
			registered:   new(pgtype.Timestamp),
			homoResult:   pgtype.Timestamp{},
			value:        "2026-07-06 03:00:00",
			originalType: "pg:classifier.local_date_t",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cp := defaultChangeProcessor()
			cp.connInfo.RegisterDataType(pgtype.DataType{
				Value: test.registered,
				Name:  test.originalType,
				OID:   uint32(test.oid),
			})

			colSchema := abstract.ColSchema{
				DataType:     "timestamp",
				OriginalType: test.originalType,
			}

			restored, err := cp.restoreType(test.value, test.oid, &colSchema)
			require.NoError(t, err)
			require.IsType(t, time.Time{}, restored)

			restoredNull, err := cp.restoreType(nil, test.oid, &colSchema)
			require.NoError(t, err)
			require.Nil(t, restoredNull)

			cp.config.IsHomo = true
			restoredHomo, err := cp.restoreType(test.value, test.oid, &colSchema)
			require.NoError(t, err)
			require.IsType(t, test.homoResult, restoredHomo)

			restoredHomoNull, err := cp.restoreType(nil, test.oid, &colSchema)
			require.NoError(t, err)
			require.Nil(t, restoredHomoNull)
		})
	}
}

func TestFixupChangePreservesOriginalValueInCastError(t *testing.T) {
	tableID := *abstract.NewTableID("classifier", "rules")
	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{
			TableSchema:  tableID.Namespace,
			TableName:    tableID.Name,
			ColumnName:   "started_at",
			DataType:     "timestamp",
			OriginalType: "pg:classifier.date_t",
		},
	})

	cp := defaultChangeProcessor()
	cp.schemasToEmit[tableID] = tableSchema
	cp.fastSchemas = fastSchemasFromDBSchema(cp.schemasToEmit)

	change := abstract.ChangeItem{
		Schema:       tableID.Namespace,
		Table:        tableID.Name,
		ColumnNames:  []string{"started_at"},
		ColumnValues: []any{"not-a-postgres-timestamp"},
	}

	err := cp.fixupChange(&change, []pgtype.OID{0}, nil, 0, 0)
	require.ErrorContains(t, err, "Can't cast value 'not-a-postgres-timestamp'")
	require.Equal(t, []any{"not-a-postgres-timestamp"}, change.ColumnValues)
}

func newColSchemaForType(originalType string) *abstract.ColSchema {
	result := new(abstract.ColSchema)
	result.OriginalType = originalType
	result.DataType = PgTypeToYTType(strings.TrimPrefix(originalType, "pg:")).String()
	return result
}

func TestRestoreArray(t *testing.T) {
	type ArrayTest struct {
		Source         string
		ExpectedResult interface{}
		TypeOID        pgtype.OID
		TypeName       string
		ElementOID     pgtype.OID
		NewElement     func() (pgtype.Value, error)
	}
	tests := []ArrayTest{
		{
			Source:         "{}",
			ExpectedResult: []interface{}{},
			TypeOID:        pgtype.Int4ArrayOID,
			TypeName:       "_int4",
			ElementOID:     pgtype.Int4OID,
			NewElement: func() (pgtype.Value, error) {
				return new(pgtype.Int4), nil
			},
		},
		{
			Source:         "{1,2,3}",
			ExpectedResult: []interface{}{int32(1), int32(2), int32(3)},
			TypeOID:        pgtype.Int4ArrayOID,
			ElementOID:     pgtype.Int4OID,
			TypeName:       "_int4",
			NewElement: func() (pgtype.Value, error) {
				return new(pgtype.Int4), nil
			},
		},
		{
			Source:         "{{foo,bar},{abc,xyz}}",
			ExpectedResult: []interface{}{[]interface{}{"foo", "bar"}, []interface{}{"abc", "xyz"}},
			TypeOID:        pgtype.TextArrayOID,
			ElementOID:     pgtype.TextOID,
			TypeName:       "_text",
			NewElement: func() (pgtype.Value, error) {
				return new(pgtype.Text), nil
			},
		},
	}

	cp := defaultChangeProcessor()
	for _, test := range tests {
		t.Run(test.Source, func(t *testing.T) {
			colSchema := newColSchemaForType("pg:" + test.TypeName + "[]")
			value, err := NewGenericArray(logger.Log, test.TypeName, test.ElementOID, test.NewElement)
			require.NoError(t, err)
			cp.connInfo.RegisterDataType(pgtype.DataType{
				Value: value,
				Name:  test.TypeName,
				OID:   uint32(test.TypeOID),
			})
			result, err := cp.restoreType(test.Source, test.TypeOID, colSchema)
			require.NoError(t, err)
			require.Equal(t, test.ExpectedResult, result)
		})
	}
}
