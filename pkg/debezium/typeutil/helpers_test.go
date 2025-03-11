package typeutil

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
)

func TestChangeItemsBitsToDebezium(t *testing.T) {
	require.Equal(t, "", ChangeItemsBitsToDebeziumHonest("00000000"))
	require.Equal(t, "AQ==", ChangeItemsBitsToDebeziumHonest("00000001"))
	require.Equal(t, "Ag==", ChangeItemsBitsToDebeziumHonest("00000010"))
	require.Equal(t, "Aw==", ChangeItemsBitsToDebeziumHonest("00000011"))
	require.Equal(t, "BA==", ChangeItemsBitsToDebeziumHonest("00000100"))
	require.Equal(t, "BQ==", ChangeItemsBitsToDebeziumHonest("00000101"))
	require.Equal(t, "Bg==", ChangeItemsBitsToDebeziumHonest("00000110"))
	require.Equal(t, "Bw==", ChangeItemsBitsToDebeziumHonest("00000111"))
	require.Equal(t, "CA==", ChangeItemsBitsToDebeziumHonest("00001000"))

	require.Equal(t, "AQ==", ChangeItemsBitsToDebeziumHonest("00000001"))
	require.Equal(t, "Ag==", ChangeItemsBitsToDebeziumHonest("00000010"))
	require.Equal(t, "BA==", ChangeItemsBitsToDebeziumHonest("00000100"))
	require.Equal(t, "CA==", ChangeItemsBitsToDebeziumHonest("00001000"))
	require.Equal(t, "EA==", ChangeItemsBitsToDebeziumHonest("00010000"))
	require.Equal(t, "IA==", ChangeItemsBitsToDebeziumHonest("00100000"))
	require.Equal(t, "QA==", ChangeItemsBitsToDebeziumHonest("01000000"))
	require.Equal(t, "gA==", ChangeItemsBitsToDebeziumHonest("10000000"))
	require.Equal(t, "wA==", ChangeItemsBitsToDebeziumHonest("11000000"))
	require.Equal(t, "4A==", ChangeItemsBitsToDebeziumHonest("11100000"))
	require.Equal(t, "8A==", ChangeItemsBitsToDebeziumHonest("11110000"))
	require.Equal(t, "+A==", ChangeItemsBitsToDebeziumHonest("11111000"))
	require.Equal(t, "/A==", ChangeItemsBitsToDebeziumHonest("11111100"))
	require.Equal(t, "/g==", ChangeItemsBitsToDebeziumHonest("11111110"))
	require.Equal(t, "rw==", ChangeItemsBitsToDebeziumHonest("10101111"))
	require.Equal(t, "/w==", ChangeItemsBitsToDebeziumHonest("11111111"))

	require.Equal(t, "roA=", ChangeItemsBitsToDebeziumHonest("1000000010101110"))
}

func TestBufToChangeItemsBits(t *testing.T) {
	require.Equal(t, BufToChangeItemsBits([]byte{0xbb}), "10111011")
}

func TestParsePostgresInterval(t *testing.T) {
	val00, err := ParsePostgresInterval("1 day 01:00:00.000000", "numeric")
	require.NoError(t, err)
	require.Equal(t, val00, uint64(90000000000))

	val01, err := ParsePostgresInterval("1 month", "numeric")
	require.NoError(t, err)
	require.Equal(t, val01, uint64(2629800000000)) // 30 days + 10.5 hours

	val02, err := ParsePostgresInterval("1 year", "numeric")
	require.NoError(t, err)
	require.Equal(t, val02, uint64(31557600000000)) // 12 months

	val03, err := ParsePostgresInterval("40 years", "numeric")
	require.NoError(t, err)
	require.Equal(t, val03, uint64(1262304000000000)) // 480 months

	val04, err := ParsePostgresInterval("14 mon 3 day 04:05:06.000007", "numeric")
	require.NoError(t, err)
	require.Equal(t, val04, uint64(37091106000007))

	val05, err := ParsePostgresInterval("14 mon 3 day 04:05:06.000070", "numeric")
	require.NoError(t, err)
	require.Equal(t, val05, uint64(37091106000070))

	val06, err := ParsePostgresInterval("1 year 2 mons 3 days 04:05:06.00007", "numeric")
	require.NoError(t, err)
	require.Equal(t, val06, uint64(37091106000070))
}

func TestEmitPostgresInterval(t *testing.T) {
	val00 := EmitPostgresInterval(90000000000)
	require.Equal(t, val00, "1 day 01:00:00.000000")

	val01 := EmitPostgresInterval(2629800000000)
	require.Equal(t, val01, "1 month", "numeric") // 30 days + 10.5 hours

	val02 := EmitPostgresInterval(31557600000000)
	require.Equal(t, val02, "1 year") // 12 months

	val03 := EmitPostgresInterval(1262304000000000)
	require.Equal(t, val03, "40 years") // 480 months
}

func TestGetBitLength(t *testing.T) {
	i2, err := GetBitLength("pg:bit(2)")
	require.NoError(t, err)
	require.Equal(t, i2, "2")

	i22, err := GetBitLength("pg:bit(22)")
	require.NoError(t, err)
	require.Equal(t, i22, "22")

	i3, err := GetBitLength("pg:bit varying(3)")
	require.NoError(t, err)
	require.Equal(t, i3, "3")

	i33, err := GetBitLength("pg:bit varying(33)")
	require.NoError(t, err)
	require.Equal(t, i33, "33")

	i4, err := GetBitLength("mysql:bit(2)")
	require.NoError(t, err)
	require.Equal(t, i4, "2")

	i44, err := GetBitLength("mysql:bit(22)")
	require.NoError(t, err)
	require.Equal(t, i44, "22")
}

func TestUnescapeUnicode(t *testing.T) {
	require.Equal(t, "<foo>bar</foo>", UnescapeUnicode("\\u003cfoo\\u003ebar\\u003c/foo\\u003e"))
}

func TestFillLeftByZeroesToAlign(t *testing.T) {
	t.Run("", func(t *testing.T) {
		_, err := fillByZeroesToAlignL("11", 1)
		require.Error(t, err)
	})

	checkL := func(t *testing.T, in string, lengthShouldBe int, expectedOut string) {
		result, err := fillByZeroesToAlignL(in, lengthShouldBe)
		require.NoError(t, err)
		require.Equal(t, expectedOut, result)
	}
	checkR := func(t *testing.T, in string, lengthShouldBe int, expectedOut string) {
		result, err := fillByZeroesToAlignR(in, lengthShouldBe)
		require.NoError(t, err)
		require.Equal(t, expectedOut, result)
	}

	checkL(t, "111", 3, "111")
	checkL(t, "11", 3, "011")
	checkL(t, "1", 3, "001")
	checkR(t, "111", 3, "111")
	checkR(t, "11", 3, "110")
	checkR(t, "1", 3, "100")
}

func helperTestExponentialFloatFormToNumeric(t *testing.T, positiveNumeric, expectedOut string) {
	q0, err := ExponentialFloatFormToNumeric(positiveNumeric)
	require.NoError(t, err)
	require.Equal(t, expectedOut, q0)

	q1, err := ExponentialFloatFormToNumeric("-" + positiveNumeric)
	require.NoError(t, err)
	require.Equal(t, "-"+expectedOut, q1)
}

func TestExponentialFloatFormToNumeric(t *testing.T) {
	helperTestExponentialFloatFormToNumeric(t, "19e-1", "1.9")
	helperTestExponentialFloatFormToNumeric(t, "191e-2", "1.91")
	helperTestExponentialFloatFormToNumeric(t, "191e-3", "0.191")
	helperTestExponentialFloatFormToNumeric(t, "191e-4", "0.0191")
	helperTestExponentialFloatFormToNumeric(t, "190", "190")
	helperTestExponentialFloatFormToNumeric(t, ".123e3", "123")
	helperTestExponentialFloatFormToNumeric(t, ".123e-14", "0.00000000000000123")
	helperTestExponentialFloatFormToNumeric(t, "1e-2", "0.01")
}

func TestNumericToExponentialFloatForm(t *testing.T) {
	q0, err := NumericToExponentialFloatForm("1.9")
	require.NoError(t, err)
	require.Equal(t, "19e-1", q0)

	q1, err := NumericToExponentialFloatForm("1.91")
	require.NoError(t, err)
	require.Equal(t, "191e-2", q1)

	q3, err := NumericToExponentialFloatForm("0.191")
	require.NoError(t, err)
	require.Equal(t, "191e-3", q3)

	q4, err := NumericToExponentialFloatForm("0.0191")
	require.NoError(t, err)
	require.Equal(t, "191e-4", q4)

	p0, err := NumericToExponentialFloatForm("190")
	require.NoError(t, err)
	require.Equal(t, "190", p0)
}

func TestNumericRangeToDebezium(t *testing.T) {
	q0, err := NumRangeToDebezium("[19e-1,191e-2)")
	require.NoError(t, err)
	require.Equal(t, "[1.9,1.91)", q0)
}

func TestNumericRangeFromDebezium(t *testing.T) {
	q0, err := NumRangeFromDebezium("[1.9,1.91)")
	require.NoError(t, err)
	require.Equal(t, "[19e-1,191e-2)", q0)
}

func TestPointToDebezium(t *testing.T) {
	q0, err := PointToDebezium("(23.4,-44.5)")
	require.NoError(t, err)
	qBytes, err := json.Marshal(q0)
	require.NoError(t, err)
	require.Equal(t, `{"srid":null,"wkb":"","x":23.4,"y":-44.5}`, string(qBytes))
}

func TestParsePgDateTimeWithTimezone(t *testing.T) {
	val, err := ParsePgDateTimeWithTimezone("2010-01-01 09:00:00+03")
	require.NoError(t, err)
	require.Equal(t, `2010-01-01 06:00:00+00`, val.UTC().Format(pgTimestampLayout3))
}

func TestDecimalToDebezium(t *testing.T) {
	result0, err := DecimalToDebezium("1e-2", "numeric(18,2)", map[string]string{debeziumparameters.DecimalHandlingMode: debeziumparameters.DecimalHandlingModePrecise})
	require.NoError(t, err)
	require.Equal(t, "AQ==", result0.(string))

	result1, err := DecimalToDebezium("10000e-2", "numeric(18,2)", map[string]string{debeziumparameters.DecimalHandlingMode: debeziumparameters.DecimalHandlingModePrecise})
	require.NoError(t, err)
	require.Equal(t, "JxA=", result1.(string))

	result2, err := DecimalToDebezium("-10000e-2", "numeric(18,2)", map[string]string{debeziumparameters.DecimalHandlingMode: debeziumparameters.DecimalHandlingModePrecise})
	require.NoError(t, err)
	require.Equal(t, "2PA=", result2.(string))
}

func helpTestDecimalToDebeziumPrimitivesImpl(t *testing.T, in, out string, expectedScale int) {
	val0, scale0, err := DecimalToDebeziumPrimitivesImpl(in)
	require.NoError(t, err)
	require.Equal(t, scale0, expectedScale)
	require.Equal(t, out, val0)
}

func TestDecimalToDebeziumPrimitivesImpl(t *testing.T) {
	helpTestDecimalToDebeziumPrimitivesImpl(t, "0.00", "AA==", 2) // TM-6908
	helpTestDecimalToDebeziumPrimitivesImpl(t, "1267650600228229401496703205376", "EAAAAAAAAAAAAAAAAA==", 0)
	helpTestDecimalToDebeziumPrimitivesImpl(t, "126765060022822940149670320537.6", "EAAAAAAAAAAAAAAAAA==", 1)
	helpTestDecimalToDebeziumPrimitivesImpl(t, "100.00", "JxA=", 2)
	helpTestDecimalToDebeziumPrimitivesImpl(t, "-100.00", "2PA=", 2)
	helpTestDecimalToDebeziumPrimitivesImpl(t, "2345678901", "AIvQODU=", 0)
	helpTestDecimalToDebeziumPrimitivesImpl(t, "-2345678901", "/3Qvx8s=", 0)
}

func TestTstZRangeQuote(t *testing.T) {
	q0, err := TstZRangeQuote(`[2010-01-01 01:00:00-05,2010-01-01 02:00:00-08)`)
	require.NoError(t, err)
	require.Equal(t, `["2010-01-01 06:00:00+00","2010-01-01 10:00:00+00")`, q0)
}

func TestTstZRangeUnquote(t *testing.T) {
	q0, err := TstZRangeUnquote(`["2010-01-01 06:00:00+00","2010-01-01 10:00:00+00")`)
	require.NoError(t, err)
	require.Equal(t, `[2010-01-01 06:00:00Z,2010-01-01 10:00:00Z)`, q0)
}

func TestTsRangeUnquote(t *testing.T) {
	q0, err := TSRangeUnquote(`["2010-01-01 06:00:00","2010-01-01 10:00:00")`)
	require.NoError(t, err)
	require.Equal(t, `[2010-01-01 06:00:00,2010-01-01 10:00:00)`, q0)
}

func TestGetTimePrecision(t *testing.T) {
	var precision int

	precision = GetTimePrecision("time(1) without time zone")
	require.Equal(t, 1, precision)
	precision = GetTimePrecision("time(3) without time zone")
	require.Equal(t, 3, precision)
	precision = GetTimePrecision("time(4) without time zone")
	require.Equal(t, 4, precision)
	precision = GetTimePrecision("time(6) without time zone")
	require.Equal(t, 6, precision)

	precision = GetTimePrecision("timestamp(1) without time zone")
	require.Equal(t, 1, precision)
	precision = GetTimePrecision("timestamp(3) without time zone")
	require.Equal(t, 3, precision)
	precision = GetTimePrecision("timestamp(4) without time zone")
	require.Equal(t, 4, precision)
	precision = GetTimePrecision("timestamp(6) without time zone")
	require.Equal(t, 6, precision)

	precision = GetTimePrecision("mysql:timestamp(0)")
	require.Equal(t, 0, precision)
	precision = GetTimePrecision("mysql:timestamp(1)")
	require.Equal(t, 1, precision)
	precision = GetTimePrecision("mysql:timestamp(6)")
	require.Equal(t, 6, precision)

	precision = GetTimePrecision("mysql:datetime(0)")
	require.Equal(t, 0, precision)
	precision = GetTimePrecision("mysql:datetime(1)")
	require.Equal(t, 1, precision)
	precision = GetTimePrecision("mysql:datetime(6)")
	require.Equal(t, 6, precision)
}

func TestFormatTime(t *testing.T) {
	currTime, err := ParseTimestamp("2006-01-02T15:04:05.654321Z")
	require.NoError(t, err)
	require.Equal(t, "2006-01-02T15:04:05Z", FormatTime(currTime, 0))
	require.Equal(t, "2006-01-02T15:04:05.6Z", FormatTime(currTime, 1))
	require.Equal(t, "2006-01-02T15:04:05.65Z", FormatTime(currTime, 2))
	require.Equal(t, "2006-01-02T15:04:05.654Z", FormatTime(currTime, 3))
	require.Equal(t, "2006-01-02T15:04:05.6543Z", FormatTime(currTime, 4))
	require.Equal(t, "2006-01-02T15:04:05.65432Z", FormatTime(currTime, 5))
	require.Equal(t, "2006-01-02T15:04:05.654321Z", FormatTime(currTime, 6))

	currTime2, err := ParseTimestamp("2006-01-02T15:04:05.12345Z")
	require.NoError(t, err)
	require.Equal(t, "2006-01-02T15:04:05.12345Z", FormatTime(currTime2, 5))

	tt, err := time.Parse("2006-01-02 15:04:05", "2022-08-09 15:00:00")
	require.NoError(t, err)
	require.Equal(t, "2022-08-09T15:00:00Z", FormatTime(tt, 3))
}

func TestGetTimeDivider(t *testing.T) {
	dividerTime1, err := GetTimeDivider("time(1) without time zone")
	require.NoError(t, err)
	require.Equal(t, 1000, dividerTime1)

	dividerTime3, err := GetTimeDivider("time(3) without time zone")
	require.NoError(t, err)
	require.Equal(t, 1000, dividerTime3)

	dividerTime4, err := GetTimeDivider("time(4) without time zone")
	require.NoError(t, err)
	require.Equal(t, 1, dividerTime4)

	dividerTime6, err := GetTimeDivider("time(6) without time zone")
	require.NoError(t, err)
	require.Equal(t, 1, dividerTime6)

	//---

	dividerTimestamp1, err := GetTimeDivider("timestamp(1) without time zone")
	require.NoError(t, err)
	require.Equal(t, 1000, dividerTimestamp1)

	dividerTimestamp3, err := GetTimeDivider("timestamp(3) without time zone")
	require.NoError(t, err)
	require.Equal(t, 1000, dividerTimestamp3)

	dividerTimestamp4, err := GetTimeDivider("timestamp(4) without time zone")
	require.NoError(t, err)
	require.Equal(t, 1, dividerTimestamp4)

	dividerTimestamp6, err := GetTimeDivider("timestamp(6) without time zone")
	require.NoError(t, err)
	require.Equal(t, 1, dividerTimestamp6)

	//---

	dividerTimeArr, err := GetTimeDivider("time without time zone[]")
	require.NoError(t, err)
	require.Equal(t, 1, dividerTimeArr)

	//---

	dividerTime1Arr, err := GetTimeDivider("time(1) without time zone[]")
	require.NoError(t, err)
	require.Equal(t, 1000, dividerTime1Arr)
}

func TestDecimalGetPrecisionAndScale(t *testing.T) {
	putScaleToValue0, precision0, scale0, err := DecimalGetPrecisionAndScale(`numeric(5,2)`)
	require.Equal(t, false, putScaleToValue0)
	require.Equal(t, 5, precision0)
	require.Equal(t, 2, scale0)
	require.NoError(t, err)

	putScaleToValue1, precision1, scale1, err := DecimalGetPrecisionAndScale(`numeric`)
	require.Equal(t, true, putScaleToValue1)
	require.Equal(t, 0, precision1)
	require.Equal(t, 0, scale1)
	require.NoError(t, err)

	putScaleToValue2, _, _, err := DecimalGetPrecisionAndScale(``)
	require.Equal(t, false, putScaleToValue2)
	require.Equal(t, 0, precision1)
	require.Equal(t, 0, scale1)
	require.NoError(t, err)

	putScaleToValue3, precision3, scale3, err := DecimalGetPrecisionAndScale(`numeric[]`)
	require.Equal(t, true, putScaleToValue3)
	require.Equal(t, 0, precision3)
	require.Equal(t, 0, scale3)
	require.NoError(t, err)
}

func TestMysqlDecimalFloatToValue(t *testing.T) {
	val0, err := MysqlDecimalFloatToValue(2345678901.000000, `mysql:decimal(10,0)`)
	require.NoError(t, err)
	require.Equal(t, "2345678901", val0)

	val1, err := MysqlDecimalFloatToValue(23451.000000, `mysql:decimal(5,0)`)
	require.NoError(t, err)
	require.Equal(t, "23451", val1)

	val2, err := MysqlDecimalFloatToValue(231.450000, `mysql:decimal(5,2)`)
	require.NoError(t, err)
	require.Equal(t, "23145", val2)
}

func TestBase64ToNumeric(t *testing.T) {
	numeric, err := Base64ToNumeric("AQ==", 2)
	require.NoError(t, err)
	require.Equal(t, "0.01", numeric)
}

func TestDateTest(t *testing.T) {
	t.Run("date", func(t *testing.T) {
		date := time.Date(2020, 2, 2, 0, 0, 0, 0, time.UTC)
		require.Equal(t, date, TimeFromDate(int64(DateToInt32(date))))
	})
	t.Run("datetime", func(t *testing.T) {
		date := time.Date(2020, 2, 2, 0, 0, 2, 0, time.UTC)
		require.Equal(t, date, TimeFromDatetime(int64(DatetimeToSecs(date))))
	})
	t.Run("timestamp", func(t *testing.T) {
		date := time.Date(2020, 2, 2, 0, 0, 0, 2000, time.UTC)
		require.Equal(t, date, TimeFromTimestamp(int64(DatetimeToMicrosecs(date))))
	})
}

func checkMysqlEnumsAndSets(t *testing.T, expected, in string) {
	result, err := UnwrapMysqlEnumsAndSets(in)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestUnwrapMysqlEnumsAndSets(t *testing.T) {
	checkMysqlEnumsAndSets(t, `x-small,small,medium,large,x-large`, `'x-small','small','medium','large','x-large'`)
}

func checkShrinkMysqlBit(t *testing.T, length int, expected string) {
	resultRaw, err := ShrinkMysqlBit([]uint8{0, 0, 0, 0, 0, 0, 0, 1}, fmt.Sprintf("mysql:bit(%d)", length))
	require.NoError(t, err)
	result, err := ParseMysqlBit(resultRaw, "")
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestShrinkMysqlBit(t *testing.T) {
	checkShrinkMysqlBit(t, 2, `AQ==`)
	checkShrinkMysqlBit(t, 8, `AQ==`)

	checkShrinkMysqlBit(t, 9, `AQA=`)
	checkShrinkMysqlBit(t, 16, `AQA=`)

	checkShrinkMysqlBit(t, 17, `AQAA`)
	checkShrinkMysqlBit(t, 24, `AQAA`)

	checkShrinkMysqlBit(t, 25, `AQAAAA==`)
	checkShrinkMysqlBit(t, 32, `AQAAAA==`)

	checkShrinkMysqlBit(t, 33, `AQAAAAA=`)
	checkShrinkMysqlBit(t, 40, `AQAAAAA=`)

	checkShrinkMysqlBit(t, 41, `AQAAAAAA`)
	checkShrinkMysqlBit(t, 48, `AQAAAAAA`)

	checkShrinkMysqlBit(t, 49, `AQAAAAAAAA==`)
	checkShrinkMysqlBit(t, 56, `AQAAAAAAAA==`)

	checkShrinkMysqlBit(t, 57, `AQAAAAAAAAA=`)
	checkShrinkMysqlBit(t, 63, `AQAAAAAAAAA=`)
}

func TestLSNToFileAndPos(t *testing.T) {
	file, pos := LSNToFileAndPos(2000000013747)
	require.Equal(t, "mysql-log.000002", file)
	require.Equal(t, uint64(13747), pos)
}

func TestSprintfDebeziumTime(t *testing.T) {
	check := func(t *testing.T, in, expectedOut string) {
		tt, err := time.Parse("2006-01-02 15:04:05.000000Z", in)
		require.NoError(t, err)
		require.Equal(t, expectedOut, SprintfDebeziumTime(tt))
	}
	check(t, "2022-08-28 19:49:47.749906Z", "2022-08-28T19:49:47.749906Z")
	check(t, "2022-08-28 19:49:47.090000Z", "2022-08-28T19:49:47.09Z")
}

func TestUserDefinedTypeToString(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		result, err := UnknownTypeToString("blablabla")
		require.NoError(t, err)
		require.Equal(t, "blablabla", result)
	})
	t.Run("int", func(t *testing.T) {
		result, err := UnknownTypeToString(1)
		require.NoError(t, err)
		require.Equal(t, "1", result)
	})
	t.Run("map[string]int", func(t *testing.T) {
		result, err := UnknownTypeToString(map[string]int{"a": 1})
		require.NoError(t, err)
		require.Equal(t, `{"a":1}`, result)
	})
	t.Run("[]int", func(t *testing.T) {
		result, err := UnknownTypeToString([]int{1, 2})
		require.NoError(t, err)
		require.Equal(t, `[1,2]`, result)
	})
}
