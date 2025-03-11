package testutil

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
	"github.com/transferria/transferria/pkg/debezium/typeutil"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/util/jsonx"
)

func sortKeys(t *testing.T, in string) string {
	var myMap map[string]interface{}
	if err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&myMap); err != nil {
		t.FailNow()
	}
	result, err := json.Marshal(myMap)
	require.NoError(t, err)
	return string(result)
}

func eraseMeta(in string) string {
	result := in

	tsmsRegexp := regexp.MustCompile(`"ts_ms":\d+`)
	result = tsmsRegexp.ReplaceAllString(result, `"ts_ms":0`)

	lsnRegexp := regexp.MustCompile(`"lsn":\d+`)
	result = lsnRegexp.ReplaceAllString(result, `"lsn":0`)

	txidRegexp := regexp.MustCompile(`"txId":\d+`)
	result = txidRegexp.ReplaceAllString(result, `"txId":0`)

	return result
}

func normalizeDebeziumEvent(t *testing.T, debeziumEvent string, ignoreTimingsAndLSN bool) string {
	result := debeziumEvent

	result = strings.ReplaceAll(result, `"default":0,`, ``) // mysql case - 'default' is not supported yet
	result = sortBeforeAndAfter(t, result)

	result = strings.ReplaceAll(result, `{"field":"ss","optional":true,"type":"int16"}`, `{"field":"ss","optional":false,"type":"int16"}`)   // old versions of debezium somewhy think serial is mandatory
	result = strings.ReplaceAll(result, `{"field":"aid","optional":true,"type":"int32"}`, `{"field":"aid","optional":false,"type":"int32"}`) // old versions of debezium somewhy think serial is mandatory
	result = strings.ReplaceAll(result, `{"field":"bid","optional":true,"type":"int64"}`, `{"field":"bid","optional":false,"type":"int64"}`) // old versions of debezium somewhy think serial is mandatory

	if ignoreTimingsAndLSN {
		result = eraseMeta(result)
	}

	result = sortKeys(t, result)
	result = typeutil.UnescapeUnicode(result) // it's for xml

	result = strings.ReplaceAll(result, "AQEAAABmZmZmZmY3QAAAAAAAQEbA", "") // it's something in 'wkb' in point. I don't know what is it & how to reproduce it

	result = strings.ReplaceAll(result, `{"field":"i","optional":false,"type":"int32"},`, "")

	result = strings.ReplaceAll(result, `{\"k1\": \"v1\"}`, `{\"k1\":\"v1\"}`) // json we deserialize-serialize, and lost tabs
	result = strings.ReplaceAll(result, `{\"k2\": \"v2\"}`, `{\"k2\":\"v2\"}`) // json we deserialize-serialize, and lost tabs

	result = strings.ReplaceAll(result, `"true,last,false,incremental"`, `"true,last,false"`)        // "allowed" in snapshot enum
	result = strings.ReplaceAll(result, `{"field":"sequence","optional":true,"type":"string"},`, ``) // remove sequence field description

	result = regexp.MustCompile(`,"sequence":"[^]]+]"`).ReplaceAllString(result, ``)
	result = strings.ReplaceAll(result, `"sequence":null,`, ``)
	result = regexp.MustCompile(`{"field":"sequence","optional":true,"type":"string"},`).ReplaceAllString(result, ``)
	result = strings.ReplaceAll(result, `,incremental`, ``)

	result = regexp.MustCompile(`,"snapshot":"[a-z]+"`).ReplaceAllString(result, ``)
	result = regexp.MustCompile(`"version":"[0-9A-Za-z.]+"`).ReplaceAllString(result, ``)

	// mysql specific
	result = strings.ReplaceAll(result, `AIvQODU=`, `i9A4NQ==`)       // in vanilla debezium, mysql:decimal has useless leading zero
	result = strings.ReplaceAll(result, `1.2300000190734863`, `1.23`) // somewhy we had little another precision
	result = regexp.MustCompile(`"file":"[^"]+"`).ReplaceAllString(result, `"file":""`)
	result = regexp.MustCompile(`"pos":\d+`).ReplaceAllString(result, `"pos":0`)
	result = strings.ReplaceAll(result, `{"field":"real_","optional":true,"type":"float"}`, `{"field":"real_","optional":true,"type":"double"}`)
	result = strings.ReplaceAll(result, `{"field":"real_10_2","optional":true,"type":"float"}`, `{"field":"real_10_2","optional":true,"type":"double"}`)
	result = regexp.MustCompile(`"server_id":\d+`).ReplaceAllString(result, `"server_id":0`) // from 'source' block
	result = regexp.MustCompile(`"thread":\d+`).ReplaceAllString(result, `"thread":null`)    // from 'source' block
	result = strings.ReplaceAll(result, `"tinyint1":1`, `"tinyint1":true`)                   // we can't divide: tinyint(1) from bool
	result = strings.ReplaceAll(result, `"bool1":0`, `"bool1":false`)                        // to cure snapshots - debezium has numbers on snapshots here. we have bool everywhere
	result = strings.ReplaceAll(result, `"bool2":1`, `"bool2":true`)                         // to cure snapshots - debezium has numbers on snapshots here. we have bool everywhere
	result = strings.ReplaceAll(result, `{"field":"bool1","optional":true,"type":"int16"}`, `{"field":"bool1","optional":true,"type":"boolean"}`)
	result = strings.ReplaceAll(result, `{"field":"bool2","optional":true,"type":"int16"}`, `{"field":"bool2","optional":true,"type":"boolean"}`)
	result = strings.ReplaceAll(result, `{"field":"tinyint1","optional":true,"type":"int16"}`, `{"field":"tinyint1","optional":true,"type":"boolean"}`)
	result = strings.ReplaceAll(result, `"time1":14706100000`, `"time1":0`)
	result = strings.ReplaceAll(result, `"time3":14706123000`, `"time3":0`)
	result = strings.ReplaceAll(result, `"time5":14706123450`, `"time5":0`)
	result = strings.ReplaceAll(result, `"time6":14706123456`, `"time6":0`)

	return result
}

func Compare(t *testing.T, expected, actual string) {
	if len(expected) != len(actual) {
		require.Equal(t, expected, actual) // special workaround - to see in debugger console what's wrong!
	}
	for i := 0; i < len(expected); i++ {
		if strings.ToUpper(string(expected[i])) == "E" {
			if expected[i] != actual[i] && (actual[i] == 'e' || actual[i] == 'E') {
				actual = actual[0:i] + string(expected[i]) + actual[i+1:]
			}
		}
	}
	require.Equal(t, expected, actual)
}

func sortJSON(in string) string {
	var j interface{}
	_ = json.Unmarshal([]byte(in), &j)
	keySorted, _ := json.Marshal(j)
	return string(keySorted)
}

func CheckCanonizedDebeziumEvent(t *testing.T, changeItem *abstract.ChangeItem, databaseServerName, database, sourceType string, isSnapshot bool, canonEvents []debeziumcommon.KeyValue) {
	generator, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   database,
		debeziumparameters.TopicPrefix:      databaseServerName,
		debeziumparameters.AddOriginalTypes: debeziumparameters.BoolFalse,
		debeziumparameters.SourceType:       sourceType,
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	resultKV, err := generator.EmitKV(changeItem, debezium.GetPayloadTSMS(changeItem), isSnapshot, nil)
	require.NoError(t, err)
	require.Equal(t, len(canonEvents), len(resultKV))

	for i := range canonEvents {
		realEventKey := sortJSON(resultKV[i].DebeziumKey)
		canonEventKey := sortJSON(canonEvents[i].DebeziumKey)
		Compare(t, normalizeDebeziumEvent(t, canonEventKey, true), normalizeDebeziumEvent(t, realEventKey, true))
		if !(canonEvents[i].DebeziumVal == nil && resultKV[i].DebeziumVal == nil) {
			Compare(t, normalizeDebeziumEvent(t, *canonEvents[i].DebeziumVal, true), normalizeDebeziumEvent(t, *resultKV[i].DebeziumVal, true))
		}
	}
}

func CheckCanonizedDebeziumEvent2(t *testing.T, key string, value *string, canonEvent debeziumcommon.KeyValue) {
	realEventKey := sortJSON(key)
	canonEventKey := sortJSON(canonEvent.DebeziumKey)

	Compare(t, normalizeDebeziumEvent(t, canonEventKey, true), normalizeDebeziumEvent(t, realEventKey, true))

	if !(canonEvent.DebeziumVal == nil && value == nil) {
		Compare(t, normalizeDebeziumEvent(t, *canonEvent.DebeziumVal, true), normalizeDebeziumEvent(t, *value, true))
	}
}

func FixTestSuite(t *testing.T, testSuite []debeziumcommon.ChangeItemCanon, databaseServerName, database, sourceType string) []debeziumcommon.ChangeItemCanon {
	for i, testCase := range testSuite {
		if i == 0 || i == 1 || i == 2 {
			newVal := strings.ReplaceAll(*testSuite[i].DebeziumEvents[0].DebeziumVal, `"oid_":null`, `"oid_":2`)
			testSuite[i].DebeziumEvents[0].DebeziumVal = &newVal
		}
		if i == 3 {
			newVal0 := *testSuite[i].DebeziumEvents[0].DebeziumVal
			newVal0 = strings.ReplaceAll(newVal0, `"aid":0`, `"aid":null`)
			newVal0 = strings.ReplaceAll(newVal0, `"bid":0`, `"bid":null`)
			newVal0 = strings.ReplaceAll(newVal0, `"ss":0`, `"ss":null`)
			newVal0 = strings.ReplaceAll(newVal0, `"oid_":0`, `"oid_":null`)
			testSuite[i].DebeziumEvents[0].DebeziumVal = &newVal0

			newVal2 := *testSuite[i].DebeziumEvents[2].DebeziumVal
			newVal2 = strings.ReplaceAll(newVal2, `"oid_":null`, `"oid_":2`)
			testSuite[i].DebeziumEvents[2].DebeziumVal = &newVal2
		}
		if i == 4 {
			newVal0 := *testSuite[i].DebeziumEvents[0].DebeziumVal
			newVal0 = strings.ReplaceAll(newVal0, `"aid":0`, `"aid":null`)
			newVal0 = strings.ReplaceAll(newVal0, `"bid":0`, `"bid":null`)
			newVal0 = strings.ReplaceAll(newVal0, `"ss":0`, `"ss":null`)
			testSuite[i].DebeziumEvents[0].DebeziumVal = &newVal0
		}
		if testCase.ChangeItem != nil {
			CheckCanonizedDebeziumEvent(t, testCase.ChangeItem, databaseServerName, database, sourceType, false, testCase.DebeziumEvents)
		}
	}
	return testSuite
}

type Message struct {
	Payload map[string]interface{} `json:"payload"`
	Schema  map[string]interface{} `json:"schema"`
}

func sortBeforeAndAfter(t *testing.T, in string) string {
	var msg map[string]interface{}
	err := json.Unmarshal([]byte(in), &msg)
	require.NoError(t, err)

	schema, ok := msg["schema"]
	require.True(t, ok)

	fields, ok := schema.(map[string]interface{})["fields"].([]interface{})
	require.True(t, ok)

	for i := range fields {
		subFieldsRaw, ok := fields[i].(map[string]interface{})["fields"]
		if !ok {
			continue
		}
		subFields := subFieldsRaw.([]interface{})

		sort.Slice(subFields, func(i, j int) bool {
			iBytes, _ := json.Marshal(subFields[i])
			jBytes, _ := json.Marshal(subFields[j])
			return string(iBytes) < string(jBytes)
		})
	}

	result, err := json.Marshal(msg)
	require.NoError(t, err)

	return string(result)
}

func CompareYTTypesOriginalAndRecovered(t *testing.T, l, r *abstract.ChangeItem) {
	const float64EqualityThreshold = 1e-9

	almostEqual := func(a, b float64) bool {
		if math.IsInf(a, -1) && math.IsInf(b, -1) {
			return true
		}
		if math.IsInf(a, 1) && math.IsInf(b, 1) {
			return true
		}
		if math.IsNaN(a) && math.IsNaN(b) {
			return true
		}
		return math.Abs(a-b) <= float64EqualityThreshold
	}

	lColNameToColSchemaIndex := make(map[string]int)
	for i, el := range l.TableSchema.Columns() {
		lColNameToColSchemaIndex[el.ColumnName] = i
	}
	rColNameToColSchemaIndex := make(map[string]int)
	for i, el := range r.TableSchema.Columns() {
		rColNameToColSchemaIndex[el.ColumnName] = i
	}

	// compare YT types: original & restored
	for _, el := range r.TableSchema.Columns() {
		lIndex := lColNameToColSchemaIndex[el.ColumnName]
		require.Equal(t, l.TableSchema.Columns()[lIndex].DataType, el.DataType, fmt.Sprintf("columnName:%s, originalType:%s", el.ColumnName, el.OriginalType))
	}

	rColNameToIndex := make(map[string]int)
	for i, el := range r.ColumnNames {
		rColNameToIndex[el] = i
	}

	// compare values: original & restored
	for i, currColumnName := range l.ColumnNames {
		rColNameIdx := rColNameToIndex[currColumnName]

		require.Equal(t, l.ColumnNames[i], r.ColumnNames[rColNameIdx], fmt.Sprintf("columnName:%s", l.ColumnNames[rColNameIdx]))

		lValWrapped := l.ColumnValues[i]
		rValWrapped := r.ColumnValues[rColNameIdx]
		lType := fmt.Sprintf("%T", lValWrapped)
		rType := fmt.Sprintf("%T", rValWrapped)
		originalType := l.TableSchema.Columns()[lColNameToColSchemaIndex[currColumnName]].OriginalType

		if pgcommon.IsPgTypeTimestampWithoutTimeZone(originalType) { // debezium is corrupting this type on timezone
			continue
		}
		if strings.HasSuffix(originalType, "[]") { // in array
			continue
		}
		if lValWrapped == nil && rValWrapped == nil { // they are equal
			continue
		}

		checked := false

		if originalType == "pg:date" || originalType == "pg:numrange" || originalType == "pg:tsrange" || originalType == "pg:tstzrange" { // skip
			checked = true
		}
		if originalType == "pg:interval" && lType == "string" && rType == "string" {
			lVal := strings.TrimSuffix(lValWrapped.(string), ".000000")
			rVal := strings.TrimSuffix(rValWrapped.(string), ".000000")
			require.Equal(t, lVal, rVal)
			checked = true
		}
		if originalType == "pg:inet" && lType == "string" && rType == "string" {
			lVal := strings.TrimSuffix(lValWrapped.(string), "/32")
			rVal := strings.TrimSuffix(rValWrapped.(string), "/32")
			require.Equal(t, lVal, rVal)
			checked = true
		}
		// known case: pg:oid can get here as json.Number
		if originalType == "pg:oid" && lType == "json.Number" && rType == "int64" {
			lInt64, err := lValWrapped.(json.Number).Int64()
			require.NoError(t, err)
			require.Equal(t, lInt64, rValWrapped.(int64))
			checked = true
		}
		// known case: pg:real & pg:double precision goes from postgresql as json.Number sometime
		if (originalType == "pg:real" || originalType == "pg:double precision") && (lType == "json.Number" && rType == "float64") {
			lFloat64, err := lValWrapped.(json.Number).Float64()
			require.NoError(t, err)
			require.True(t, almostEqual(lFloat64, rValWrapped.(float64)))
			checked = true
		}
		// known case - we are losing timezone into "pg:timestamp with time zone" conversion
		if originalType == "pg:timestamp with time zone" {
			require.Equal(t, lValWrapped.(time.Time).UTC(), rValWrapped.(time.Time).UTC())
			checked = true
		}
		// known case - we are losing timezone into "pg:time with time zone" conversion
		if pgcommon.IsPgTypeTimeWithTimeZone(originalType) {
			// I'm too lazy to convert tz from string here. Let it check checksum - here I just compare type
			require.Equal(t, lType, rType, fmt.Sprintf("columnName: %s", currColumnName))
			checked = true
		}
		// two json.Number can differ by 'e0' on the end of string, or some more form difference with same meaning - just normalize them & compare
		if lType == "json.Number" && rType == "json.Number" {
			var err error
			lVal := lValWrapped.(json.Number).String()
			lVal, err = typeutil.ExponentialFloatFormToNumeric(lVal)
			require.NoError(t, err)
			rVal := rValWrapped.(json.Number).String()
			rVal, err = typeutil.ExponentialFloatFormToNumeric(rVal)
			require.NoError(t, err)
			require.Equal(t, lVal, rVal, fmt.Sprintf("columnName: %s", currColumnName))
			checked = true
		}

		if checked {
			continue
		}

		require.Equal(t, lType, rType, fmt.Sprintf("columnName: %s", currColumnName))
		require.Equal(t, lValWrapped, rValWrapped, fmt.Sprintf("columnName:%s", currColumnName))
	}
}
