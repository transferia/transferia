package tests

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/generic"
	_ "github.com/transferia/transferia/pkg/parsers/registry"
	jsonparser "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/parsers/registry/tskv"
	"github.com/transferia/transferia/pkg/parsers/tests/samples"
	"github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/pkg/stats"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

func parserConfigStructToMap(t *testing.T, parserConfigUnwrapped parsers.AbstractParserConfig) map[string]interface{} {
	resultMap, err := parsers.ParserConfigStructToMap(parserConfigUnwrapped)
	require.NoError(t, err)
	return resultMap
}

func parserConfigMapToStruct(t *testing.T, parserConfigMap map[string]interface{}) parsers.AbstractParserConfig {
	parserConfig, err := parsers.ParserConfigMapToStruct(parserConfigMap)
	require.NoError(t, err)
	return parserConfig
}

func ParserConfigMap(name string) map[string]interface{} {
	var source kafka.KafkaSource
	_ = json.Unmarshal([]byte(samples.Configs[name]), &source)
	source.WithDefaults()
	return source.ParserConfig
}

func GetGenericParserImpl(in parsers.Parser) *generic.GenericParser {
	wrapper := in.(parsers.WrappedParser)
	return wrapper.Unwrap().(*generic.GenericParser)
}

// tests

func TestParser_Do(t *testing.T) {
	parser, err := parsers.NewParserFromMap(ParserConfigMap(samples.MetrikaSample), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
		"id": "TestParser_Do",
	})))
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.MetrikaSample], abstract.Partition{})
	require.Equal(t, 1, len(res))
}

func TestParser_TableSplitter(t *testing.T) {
	parserConfigMap := ParserConfigMap(samples.MetrikaSample)

	testCase := func(cols []string) string {
		parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*tskv.ParserConfigTSKVLb)
		parserConfigUnwrapped.TableSplitter = &abstract.TableSplitter{
			Columns: cols,
		}
		parserConfigMapFinal := parserConfigStructToMap(t, parserConfigUnwrapped)

		parser, err := parsers.NewParserFromMap(parserConfigMapFinal, false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
			"id": "TestParser_Do",
		})))
		require.NoError(t, err)
		res := parser.Do(samples.Data[samples.MetrikaSample], abstract.Partition{
			Cluster:   "logbroker",
			Partition: 777,
			Topic:     "my/lovely/topic",
		})
		require.Equal(t, 1, len(res))
		return res[0].Table
	}
	t.Run("utf8", func(t *testing.T) {
		require.Equal(t, `{"cluster":"logbroker","partition":777,"topic":"my/lovely/topic"}`, testCase([]string{"_partition"}))
	})
	t.Run("[]byte", func(t *testing.T) {
		require.Equal(t, `iOS`, testCase([]string{"AppPlatform"}))
	})
	t.Run("utf8 and int64", func(t *testing.T) {
		require.Equal(t, `iOS/geoadv.bb.pin.show/402`, testCase([]string{"AppPlatform", "EventName", "AppBuildNumber"}))
	})
}

func TestParser_TableSplitterSpecChar(t *testing.T) {
	testTableName := testTableSplitterOnChangeItem(t, "test", []string{"test"}, map[string]interface{}{
		"test": []byte("utf8"),
	})
	require.Equal(t, "utf8", testTableName)

	test1TableName := testTableSplitterOnChangeItem(t, "test1", []string{"AppVersionName"}, map[string]interface{}{
		"AppVersionName": []byte("valueAux"),
	})
	require.Equal(t, "valueAux", test1TableName)

	test2TableName := testTableSplitterOnChangeItem(t, "test2", []string{"AppVersionName"}, map[string]interface{}{})
	require.Equal(t, "", test2TableName)

	test3TableName := testTableSplitterOnChangeItem(t, "test3", []string{"AppVersionName", "APIKey", "ReceiveTimestamp"}, map[string]interface{}{
		"AppVersionName":   "valueCI",
		"ReceiveTimestamp": "valueAux2",
		"APIKey":           654,
	})
	require.Equal(t, "valueCI/654/valueAux2", test3TableName)

	test4TableName := testTableSplitterOnChangeItem(t, "test4", []string{}, map[string]interface{}{
		"ReceiveTimestamp": "valueAux2",
	})
	require.Equal(t, "test4", test4TableName)

	test5TableName := testTableSplitterOnChangeItem(t, "test5", []string{"nonexistent_column"}, map[string]interface{}{
		"ReceiveTimestamp": "valueAux2",
	})
	require.Equal(t, "", test5TableName)

	// this test is dependend where you launch for some reason: notebook/qyp -- OK, distbuild -- not OK
	// Notebook: 2023-08-31 16:59:07 +0300 MSK
	// QYP: 2023-08-31 16:59:07 +0300 MSK
	// Distbuild: 2023-08-31 16:59:07 +0300 +0300 -- WHY?
	timeAsString := "2023-08-31T16:59:07+03:00"
	timestamp, err := time.Parse(time.RFC3339, timeAsString)
	require.NoError(t, err)
	test6TableName := testTableSplitterOnChangeItem(t, "test6", []string{"DeviceID"}, map[string]interface{}{
		"DeviceID": timestamp,
	})
	test6TableExpectedPrefix := "2023-08-31 16:59:07"
	require.Equal(t, test6TableName[:len(test6TableExpectedPrefix)], test6TableExpectedPrefix)
}

func testTableSplitterOnChangeItem(
	t *testing.T,
	originalTableName string,
	columns []string,
	item map[string]interface{},
) string {
	parserConfigMap := ParserConfigMap(samples.MetrikaSample)
	parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*tskv.ParserConfigTSKVLb)
	parserConfigUnwrapped.TableSplitter = &abstract.TableSplitter{
		Columns: columns,
	}
	parserConfigMapFinal := parserConfigStructToMap(t, parserConfigUnwrapped)

	parser, err := parsers.NewParserFromMap(parserConfigMapFinal, false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
		"id": "TestParser_Do",
	})))
	require.NoError(t, err)
	for {
		if rp, ok := parser.(*parsers.ResourceableParser); ok {
			parser = rp.Unwrap()
		} else {
			break
		}
	}
	gparser, ok := parser.(*generic.GenericParser)
	require.True(t, ok)

	var columnValues []interface{}
	return gparser.TableSplitter(originalTableName, item, columnValues)
}

func TestParser_DoTM280(t *testing.T) {
	parser, err := parsers.NewParserFromMap(ParserConfigMap(samples.TM280Sample), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
		"id": "TestParser_DoTM280",
	})))
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.TM280Sample], abstract.Partition{})
	require.Equal(t, 2, len(res))
	require.Equal(t, res[0].ColumnValues[0], uint64(960372025831085293))
	require.Equal(t, res[1].ColumnValues[0], uint64(18446744073709551615))
}

func TestGenericParser_DoMetrikaComplex(t *testing.T) {
	parser, err := parsers.NewParserFromMap(ParserConfigMap(samples.MetikaComplexSample), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
		"id": "TestGenericParser_DoMetrikaComplex",
	})))
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.MetikaComplexSample], abstract.Partition{})
	require.Equal(t, 1, len(res))
}

func TestGenericParser_DoSensitive(t *testing.T) {
	parser, err := parsers.NewParserFromMap(ParserConfigMap(samples.SensitiveSample), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
		"id": "TestGenericParser_DoSensitive",
	})))

	if xerrors.Is(err, parsers.UnknownParserErr) {
		// some parsers tests might be disabled in certain setups
		t.Skip()
	}
	require.NoError(t, err)
	resArr := parser.Do(samples.Data[samples.SensitiveSample], abstract.Partition{})
	require.Equal(t, 1, len(resArr))
	res := resArr[0]
	fieldsWithSecretErasure := 0
	for _, val := range res.ColumnValues {
		if strVal, ok := val.(string); ok {
			if strings.Contains(strVal, "XXXXX") {
				fieldsWithSecretErasure++
			}
		}
	}
	logger.Log.Debug("Example of secret erasure", log.Any("res", res))
	require.Greater(t, fieldsWithSecretErasure, 0, "there should be at least one erasured secret")
}

func TestGenericParser_DoSensitiveDisabled(t *testing.T) {
	parser, err := parsers.NewParserFromMap(ParserConfigMap(samples.SensitiveDisabledSample), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
		"id": "TestGenericParser_DoSensitiveDisabled",
	})))

	if xerrors.Is(err, parsers.UnknownParserErr) {
		// some parsers tests might be disabled in certain setups
		t.Skip()
	}
	require.NoError(t, err)
	resArr := parser.Do(samples.Data[samples.SensitiveDisabledSample], abstract.Partition{})
	require.Equal(t, 1, len(resArr))
	res := resArr[0]
	fieldsWithSecretErasure := 0
	for _, val := range res.ColumnValues {
		if strVal, ok := val.(string); ok {
			if strings.Contains(strVal, "XXXXX") {
				fieldsWithSecretErasure++
			}
		}
	}
	logger.Log.Debug("Example of keeping secrets unerasured to increase performance", log.Any("res", res))
	require.Equal(t, 0, fieldsWithSecretErasure, "secrets should not be processed in order to increase performance")
}

func TestGenericParser_DoKikimr(t *testing.T) {
	parser, err := parsers.NewParserFromMap(ParserConfigMap(samples.KikimrSample), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
		"id": "TestGenericParser_DoKikimr",
	})))

	if xerrors.Is(err, parsers.UnknownParserErr) {
		// some parsers tests might be disabled in certain setups
		t.Skip()
	}
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.KikimrSample], abstract.Partition{})
	require.Equal(t, 213, len(res))
}

func TestGenericParser_DoKikimrNew(t *testing.T) {
	parser, err := parsers.NewParserFromMap(ParserConfigMap(samples.KikimrNew), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
		"id": "TestGenericParser_DoKikimrNew",
	})))

	if xerrors.Is(err, parsers.UnknownParserErr) {
		// some parsers tests might be disabled in certain setups
		t.Skip()
	}
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.KikimrNew], abstract.Partition{})
	abstract.Dump(res)
	if len(res) > 0 {
		require.Equal(t, 6, len(res[len(res)-1].ColumnValues))
	}
}

func TestParser_DoJson(t *testing.T) {
	parser, err := parsers.NewParserFromMap(ParserConfigMap(samples.JSONSample), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
		"id": "TestGenericParser_DoJson",
	})))
	require.NoError(t, err)
	t.Run("change items", func(t *testing.T) {
		res := parser.Do(samples.Data[samples.JSONSample], *new(abstract.Partition))
		require.Equal(t, 36, len(res))
		offset := uint64(0)
		for _, row := range res {
			ok := false
			for ci, cname := range row.ColumnNames {
				if cname == "version" {
					require.Equal(t, 89488198116272410+offset, row.ColumnValues[ci])
					ok = true
					break
				}
			}
			offset++
			require.True(t, ok, "Should have column 'version'")
		}
	})
}

func TestMdbSample(t *testing.T) {
	parser, err := parsers.NewParserFromMap(ParserConfigMap(samples.MdbSample), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.MdbSample], abstract.Partition{})
	abstract.Dump(res)
	if len(res) > 0 {
		require.Equal(t, 29, len(res[len(res)-1].ColumnValues))
	}
}

func TestTSKVWithEbmedNewLine(t *testing.T) {
	parser, err := parsers.NewParserFromMap(ParserConfigMap(samples.TM5249), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
		"id": "TestParser_Do",
	})))
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.TM5249], abstract.Partition{})
	abstract.Dump(res)
	require.Equal(t, 2, len(res))
}

func TestLogfellerTimestampParse(t *testing.T) {
	parserName := samples.LogfellerTimestamps
	parser, err := parsers.NewParserFromMap(
		ParserConfigMap(parserName),
		false,
		logger.Log,
		stats.NewSourceStats(
			metrics.NewRegistry().WithTags(
				map[string]string{"id": "TestLogfellerTimestampParse"},
			),
		),
	)

	if xerrors.Is(err, parsers.UnknownParserErr) {
		// some parsers tests might be disabled in certain setups
		t.Skip()
	}
	require.NoError(t, err)
	require.NotNil(t, parser)

	{
		// abstract 1 parser
		res := parser.Do(samples.Data[parserName], *new(abstract.Partition))
		require.Equal(t, 1, len(res))
		fields := res[0].AsMap()
		// Timestamps
		require.Equal(t, schema.Timestamp(1234567890123456).Time().UTC(), fields["default"])
		require.Equal(t, schema.Timestamp(1234567890123456).Time().UTC(), fields["microseconds"])
		require.Equal(t, schema.Timestamp(1234567890123000).Time().UTC(), fields["milliseconds"])
		require.Equal(t, schema.Timestamp(1234567890000000).Time().UTC(), fields["seconds"])

		// Datetime (seconds)
		require.Equal(t, time.Unix(1234567890, 0), fields["datetime_default"].(time.Time))
		require.Equal(t, time.Unix(1234567890, 0), fields["datetime_microseconds"].(time.Time))
		require.Equal(t, time.Unix(1234567890, 0), fields["datetime_milliseconds"].(time.Time))
		require.Equal(t, time.Unix(1234567890, 0), fields["datetime_seconds"].(time.Time))

		// Intervals
		require.Equal(t, time.Duration(12345678000), fields["interval_default"])
		require.Equal(t, time.Duration(12345678000), fields["interval_microseconds"])
		require.Equal(t, time.Duration(12345000000), fields["interval_milliseconds"])
		require.Equal(t, time.Duration(12000000000), fields["interval_seconds"])
	}
}

func TestGenericParser_Parse_vs_Do(t *testing.T) {
	for k := range samples.Configs {
		t.Run(k, func(t *testing.T) {
			var source kafka.KafkaSource
			_ = json.Unmarshal([]byte(samples.Configs[k]), &source)
			if parsers.GetParserNameByMap(source.ParserConfig) == "yql.lb" { // skip YQL-parser, test only generic & logfeller parsers
				return
			}
			configMap := ParserConfigMap(k)
			parser, err := parsers.NewParserFromMap(configMap, false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
				"id": "TestGenericParser_Parse_vs_Do",
			})))
			if xerrors.Is(err, parsers.UnknownParserErr) {
				// some parsers tests might be disabled in certain setups
				t.Skip()
			}
			require.NoError(t, err)
			if parser == nil {
				return
			}
			require.NoError(t, err)
			res := parser.Do(samples.Data[k], *new(abstract.Partition))
			var changes []abstract.ChangeItem
			t.Logf("old parser: %v len", len(res))
			abstract.Dump(res)
			t.Logf("new parser: %v len", len(changes))
			abstract.Dump(changes)
		})
	}
}

func TestGenericParserDatetimeZone(t *testing.T) {
	parserConfigStruct := &jsonparser.ParserConfigJSONLb{
		Fields: []abstract.ColSchema{
			{ColumnName: "timestamp", DataType: schema.TypeDatetime.String()},
		},
		TimeField: &abstract.TimestampCol{
			Col:    "timestamp",
			Format: "2006-01-02T15:04:05.000Z",
		},
	}
	configMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)
	parser, err := parsers.NewParserFromMap(configMap, false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)

	res := parser.Do(parsers.Message{
		Value: []byte("{\"timestamp\":\"2025-02-19T15:43:30.089Z\"}"),
	}, abstract.Partition{Partition: 0, Topic: "test/topic"})

	require.NotEmpty(t, res)
	require.NotEmpty(t, res[0].ColumnValues)

	tsIdx := -1
	for idx, col := range res[0].ColumnNames {
		if col == "timestamp" {
			tsIdx = idx
		}
	}
	require.True(t, tsIdx >= 0)

	ts, ok := res[0].ColumnValues[tsIdx].(time.Time)
	require.True(t, ok)
	require.Equal(t, time.UTC.String(), ts.Location().String())
}

func BenchmarkGenericParser(b *testing.B) {
	for _, testCase := range []string{
		samples.MetrikaSample, // tskv
		samples.TaxiSample,    // tskv
		samples.MdbSample,     // json
		samples.JSONSample,    // json
	} {
		parser, err := parsers.NewParserFromMap(ParserConfigMap(testCase), false, logger.LoggerWithLevel(zapcore.WarnLevel), stats.NewSourceStats(metrics.NewRegistry()))
		require.NoError(b, err)
		for _, size := range []int{1, 5, 10} {
			d := samples.Data[testCase]
			for i := 1; i < size; i++ {
				d.Value = append(d.Value, []byte("\n")...)
				d.Value = append(d.Value, d.Value...)
			}
			b.Run(fmt.Sprintf("Abstract1 %v %v", testCase, size), func(b *testing.B) {
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					r := parser.Do(d, abstract.Partition{})
					require.True(b, len(r) > 0)
				}
				b.SetBytes(int64(len(d.Value)))
				b.ReportAllocs()
			})
		}
	}
}

func BenchmarkGenericParser_Do(b *testing.B) {
	for _, testCase := range []string{
		samples.JSONSample,      // json
		samples.MetrikaSample,   // tskv
		samples.TaxiSample,      // tskv
		samples.SensitiveSample, // logfeller
		samples.KikimrSample,    // logfeller
	} {
		for _, size := range []int{1} {
			b.Run(fmt.Sprint(testCase, "/", size), func(b *testing.B) {
				parser, err := parsers.NewParserFromMap(ParserConfigMap(testCase), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
				require.NoError(b, err)
				d := samples.Data[testCase]
				for i := 1; i < size; i++ {
					d.Value = append(d.Value, []byte("\n")...)
					d.Value = append(d.Value, d.Value...)
				}
				logger.Log.Debugf("input size: %v", format.SizeInt(len(d.Value)))
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					r := parser.Do(d, abstract.Partition{})
					require.True(b, len(r) > 0)
				}
				b.SetBytes(int64(len(d.Value)))
				b.ReportAllocs()
			})
		}
	}
}

func TestParseVal(t *testing.T) {
	parserW, err := parsers.NewParserFromMap(ParserConfigMap(samples.MdbSample), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)

	parser := GetGenericParserImpl(parserW)

	// v.(float64)

	doubleRes, doubleErr := parser.ParseVal(1.5, "double")
	require.NoError(t, doubleErr)
	require.Equal(t, 1.5, doubleRes)

	int8Res, int8Err := parser.ParseVal(3.0, "int8")
	require.NoError(t, int8Err)
	require.Equal(t, int8(3), int8Res)

	int16Res, int16Err := parser.ParseVal(4.0, "int16")
	require.NoError(t, int16Err)
	require.Equal(t, int16(4), int16Res)

	int32Res, int32Err := parser.ParseVal(5.0, "int32")
	require.NoError(t, int32Err)
	require.Equal(t, int32(5), int32Res)

	int64Res, int64Err := parser.ParseVal(6.0, "int64")
	require.NoError(t, int64Err)
	require.Equal(t, int64(6), int64Res)

	uint8Res, uint8Err := parser.ParseVal(7.0, "uint8")
	require.NoError(t, uint8Err)
	require.Equal(t, uint8(7), uint8Res)

	uint16Res, uint16Err := parser.ParseVal(8.0, "uint16")
	require.NoError(t, uint16Err)
	require.Equal(t, uint16(8), uint16Res)

	uint32Res, uint32Err := parser.ParseVal(9.0, "uint32")
	require.NoError(t, uint32Err)
	require.Equal(t, uint32(9), uint32Res)

	uint64Res, uint64Err := parser.ParseVal(10.0, "uint64")
	require.NoError(t, uint64Err)
	require.Equal(t, uint64(10), uint64Res)

	// json.Number

	doubleResJ, doubleErrJ := parser.ParseVal(json.Number("1.5"), "double")
	require.NoError(t, doubleErrJ)
	require.Equal(t, 1.5, doubleResJ)
	_, doubleErrJz := parser.ParseVal(json.Number("1.5z"), "double")
	require.Error(t, doubleErrJz)

	int8ResJ, int8ErrJ := parser.ParseVal(json.Number("3"), "int8")
	require.NoError(t, int8ErrJ)
	require.Equal(t, int8(3), int8ResJ)
	_, int8ErrJz := parser.ParseVal(json.Number("3z"), "int8")
	require.Error(t, int8ErrJz)

	int16ResJ, int16ErrJ := parser.ParseVal(json.Number("4"), "int16")
	require.NoError(t, int16ErrJ)
	require.Equal(t, int16(4), int16ResJ)
	_, int16ErrJz := parser.ParseVal(json.Number("4z"), "int16")
	require.Error(t, int16ErrJz)

	int32ResJ, int32ErrJ := parser.ParseVal(json.Number("5"), "int32")
	require.NoError(t, int32ErrJ)
	require.Equal(t, int32(5), int32ResJ)
	_, int32ErrJz := parser.ParseVal(json.Number("5z"), "int32")
	require.Error(t, int32ErrJz)

	int64ResJ, int64ErrJ := parser.ParseVal(json.Number("6"), "int64")
	require.NoError(t, int64ErrJ)
	require.Equal(t, int64(6), int64ResJ)
	_, int64ErrJz := parser.ParseVal(json.Number("6z"), "int64")
	require.Error(t, int64ErrJz)

	uint8ResJ, uint8ErrJ := parser.ParseVal(json.Number("7"), "uint8")
	require.NoError(t, uint8ErrJ)
	require.Equal(t, uint8(7), uint8ResJ)
	_, uint8ErrJz := parser.ParseVal(json.Number("7z"), "uint8")
	require.Error(t, uint8ErrJz)

	uint16ResJ, uint16ErrJ := parser.ParseVal(json.Number("8"), "uint16")
	require.NoError(t, uint16ErrJ)
	require.Equal(t, uint16(8), uint16ResJ)
	_, uint16ErrJz := parser.ParseVal(json.Number("8z"), "uint16")
	require.Error(t, uint16ErrJz)

	uint32ResJ, uint32ErrJ := parser.ParseVal(json.Number("9"), "uint32")
	require.NoError(t, uint32ErrJ)
	require.Equal(t, uint32(9), uint32ResJ)
	_, uint32ErrJz := parser.ParseVal(json.Number("9z"), "uint32")
	require.Error(t, uint32ErrJz)

	uint64ResJ, uint64ErrJ := parser.ParseVal(json.Number("10"), "uint64")
	require.NoError(t, uint64ErrJ)
	require.Equal(t, uint64(10), uint64ResJ)
	_, uint64ErrJz := parser.ParseVal(json.Number("10z"), "uint64")
	require.Error(t, uint64ErrJz)

	// string

	doubleResS, doubleErrS := parser.ParseVal("1.5", "double")
	require.NoError(t, doubleErrS)
	require.Equal(t, 1.5, doubleResS)
	_, doubleErrSz := parser.ParseVal("1.5z", "double")
	require.Error(t, doubleErrSz)

	int8ResS, int8ErrS := parser.ParseVal("3", "int8")
	require.NoError(t, int8ErrS)
	require.Equal(t, int8(3), int8ResS)
	_, int8ErrSz := parser.ParseVal("3z", "int8")
	require.Error(t, int8ErrSz)

	int16ResS, int16ErrS := parser.ParseVal("4", "int16")
	require.NoError(t, int16ErrS)
	require.Equal(t, int16(4), int16ResS)
	_, int16ErrSz := parser.ParseVal("4z", "int16")
	require.Error(t, int16ErrSz)

	int32ResS, int32ErrS := parser.ParseVal("5", "int32")
	require.NoError(t, int32ErrS)
	require.Equal(t, int32(5), int32ResS)
	_, int32ErrSz := parser.ParseVal("5z", "int32")
	require.Error(t, int32ErrSz)

	int64ResS, int64ErrS := parser.ParseVal("6", "int64")
	require.NoError(t, int64ErrS)
	require.Equal(t, int64(6), int64ResS)
	_, int64ErrSz := parser.ParseVal("6z", "int64")
	require.Error(t, int64ErrSz)

	uint8ResS, uint8ErrS := parser.ParseVal("7", "uint8")
	require.NoError(t, uint8ErrS)
	require.Equal(t, uint8(7), uint8ResS)
	_, uint8ErrSz := parser.ParseVal("7z", "uint8")
	require.Error(t, uint8ErrSz)

	uint16ResS, uint16ErrS := parser.ParseVal("8", "uint16")
	require.NoError(t, uint16ErrS)
	require.Equal(t, uint16(8), uint16ResS)
	_, uint16ErrSz := parser.ParseVal("8z", "uint16")
	require.Error(t, uint16ErrSz)

	uint32ResS, uint32ErrS := parser.ParseVal("9", "uint32")
	require.NoError(t, uint32ErrS)
	require.Equal(t, uint32(9), uint32ResS)
	_, uint32ErrSz := parser.ParseVal("9z", "uint32")
	require.Error(t, uint32ErrSz)

	uint64ResS, uint64ErrS := parser.ParseVal("10", "uint64")
	require.NoError(t, uint64ErrS)
	require.Equal(t, uint64(10), uint64ResS)
	_, uint64ErrSz := parser.ParseVal("10z", "uint64")
	require.Error(t, uint64ErrSz)
}
