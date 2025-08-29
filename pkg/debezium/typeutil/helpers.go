package typeutil

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/util"
)

//---------------------------------------------------------------------------------------------------------------------
// it's fixed in higher version of debezium (in 1.1 this bug is present, in 1.8 absent)
// so, actual function - changeItemsBitsToDebeziumHonest

/*
 func changeItemsBitsToDebeziumWA(bits string) string {
	 bufSize := imitateDebeziumBufSize(len(bits))
	 return changeItemsBitsStringToDebezium(bits, bufSize)
 }

func imitateDebeziumBufSize(bitsCount int) int {
	if bitsCount < 16 {
		return 2
	} else if bitsCount < 32 {
		return 4
	} else if bitsCount < 64 {
		return 8
	} else {
		return int(math.Ceil(float64(bitsCount) / 8)) // honest count
	}
}
*/

//---------------------------------------------------------------------------------------------------------------------

func ChangeItemsBitsToDebeziumHonest(bits string) string {
	honestBufSize := int(math.Ceil(float64(len(bits)) / 8))
	return changeItemsBitsStringToDebezium(bits, honestBufSize)
}

func ReverseBytesArr(buf []byte) []byte {
	result := make([]byte, len(buf))
	for i := 0; i < len(buf); i++ {
		result[i] = buf[len(buf)-1-i]
	}
	return result
}

func changeItemsBitsStringToDebezium(bits string, bufSize int) string {
	buf := make([]byte, bufSize)
	foundOne := false
	for i := len(bits) - 1; i >= 0; i-- {
		val := bits[i]
		byteNum := i / 8
		bitNum := 7 - (i % 8)
		if val == '1' {
			foundOne = true
			buf[byteNum] = buf[byteNum] | (1 << bitNum)
		}
	}
	if !foundOne {
		return ""
	}
	buf = ReverseBytesArr(buf)
	return base64.StdEncoding.EncodeToString(buf)
}

func GetBitLength(pgType string) (string, error) {
	result := ""
	if strings.HasPrefix(pgType, "pg:bit(") {
		rightIndex := strings.Index(pgType[7:], ")")
		if rightIndex == -1 {
			return "", xerrors.Errorf("unsupported pg type, can't find closing bracket: %s", pgType)
		}
		result = pgType[7 : 7+rightIndex]
	} else if strings.HasPrefix(pgType, "pg:bit varying(") {
		rightIndex := strings.Index(pgType[15:], ")")
		if rightIndex == -1 {
			return "", xerrors.Errorf("unsupported pg type, can't find closing bracket: %s", pgType)
		}
		result = pgType[15 : 15+rightIndex]
	} else if strings.HasPrefix(pgType, "mysql:bit(") {
		rightIndex := strings.Index(pgType[10:], ")")
		if rightIndex == -1 {
			return "", xerrors.Errorf("unsupported pg type, can't find closing bracket: %s", pgType)
		}
		result = pgType[10 : 10+rightIndex]
	} else {
		return "", xerrors.Errorf("unsupported pg type: %s", pgType)
	}
	return result, nil
}

func GetTimeDivider(originalTypeWithoutProvider string) (int, error) {
	if strings.HasPrefix(originalTypeWithoutProvider, "time without time zone") || strings.HasPrefix(originalTypeWithoutProvider, "timestamp without time zone") {
		return 1, nil
	}

	precision := GetTimePrecision(originalTypeWithoutProvider)

	if precision == -1 {
		return 0, xerrors.Errorf("unable to match any pattern to string: %s", originalTypeWithoutProvider)
	}

	if precision >= 1 && precision <= 3 {
		return 1000, nil
	} else {
		return 1, nil
	}
}

var (
	reTimeWithoutTZ      = regexp.MustCompile(`^time\((\d)\) without time zone`)
	reTimestampWithoutTZ = regexp.MustCompile(`^timestamp\((\d)\) without time zone`)
	reMysqlTime          = regexp.MustCompile(`^mysql:timestamp\((\d)\)`)
	reMysqlDatetime      = regexp.MustCompile(`^mysql:datetime\((\d)\)`)
)

func GetTimePrecision(colTypeStr string) int {
	precision := -1
	var precisionStrArr []string

	if colTypeStr == "timestamp without time zone" {
		return 6
	}

	precisionStrArr = reTimeWithoutTZ.FindStringSubmatch(colTypeStr)
	if len(precisionStrArr) == 2 {
		precision, _ = strconv.Atoi(precisionStrArr[1])
	}

	precisionStrArr = reTimestampWithoutTZ.FindStringSubmatch(colTypeStr)
	if len(precisionStrArr) == 2 {
		precision, _ = strconv.Atoi(precisionStrArr[1])
	}

	precisionStrArr = reMysqlTime.FindStringSubmatch(colTypeStr)
	if len(precisionStrArr) == 2 {
		precision, _ = strconv.Atoi(precisionStrArr[1])
	}

	precisionStrArr = reMysqlDatetime.FindStringSubmatch(colTypeStr)
	if len(precisionStrArr) == 2 {
		precision, _ = strconv.Atoi(precisionStrArr[1])
	}

	return precision
}

func FieldDescrDecimal(precision, scale int) (string, string, map[string]interface{}) {
	result := make(map[string]interface{})
	parameters := map[string]interface{}{
		"scale":                     fmt.Sprintf(`%d`, scale),
		"connect.decimal.precision": fmt.Sprintf(`%d`, precision),
	}
	result["parameters"] = parameters
	return "bytes", "org.apache.kafka.connect.data.Decimal", result
}

var reNumeric = regexp.MustCompile(`^numeric\((\d+),(\d+)\)`)

func DecimalGetPrecisionAndScale(dataTypeVerbose string) (bool, int, int, error) {
	if dataTypeVerbose == "" {
		return false, 0, 0, nil
	}

	if dataTypeVerbose == "numeric" {
		return true, 0, 0, nil
	}

	if strings.HasPrefix(dataTypeVerbose, "numeric[]") {
		return true, 0, 0, nil
	}

	arr := reNumeric.FindStringSubmatch(dataTypeVerbose)
	if len(arr) == 3 {
		precision, _ := strconv.Atoi(arr[1])
		scale, _ := strconv.Atoi(arr[2])
		return false, precision, scale, nil
	}

	return true, 0, 0, xerrors.Errorf("unable to parse dataTypeVerbose: %s", dataTypeVerbose)
}

var reMysqlDecimal = regexp.MustCompile(`^mysql:decimal\((\d+),(\d+)\)`)

func mysqlParsePrecisionScale(colType string) (int, int, error) {
	arr := reMysqlDecimal.FindStringSubmatch(colType)
	if len(arr) == 3 {
		precision, err := strconv.Atoi(arr[1])
		if err != nil {
			return 0, 0, xerrors.Errorf("unable to parse precision: %w", err)
		}
		scale, err := strconv.Atoi(arr[2])
		if err != nil {
			return 0, 0, xerrors.Errorf("unable to parse scale: %w", err)
		}
		return precision, scale, nil
	}
	return 0, 0, xerrors.Errorf("unable to match mysql:decimal type, type: %s", colType)
}

func MysqlDecimalFieldDescr(colSchema *abstract.ColSchema, _, _ bool, connectorParameters map[string]string) (string, string, map[string]interface{}) {
	switch debeziumparameters.GetDecimalHandlingMode(connectorParameters) {
	case debeziumparameters.DecimalHandlingModePrecise:
		precision, scale, _ := mysqlParsePrecisionScale(colSchema.OriginalType)
		return FieldDescrDecimal(precision, scale)
	case debeziumparameters.DecimalHandlingModeDouble:
		return "double", "", nil
	case debeziumparameters.DecimalHandlingModeString:
		return "string", "", nil
	default:
		return "", "", nil
	}
}

func MysqlDecimalFloatToValue(val float64, colType string) (string, error) {
	precision, scale, err := mysqlParsePrecisionScale(colType)
	if err != nil {
		return "", xerrors.Errorf("unable to extract scale&precision from mysql:decimal, type: %s, err: %w", colType, err)
	}

	decimalValStr := fmt.Sprintf("%f", val)
	arr := strings.Split(decimalValStr, ".")
	if len(arr) != 2 {
		return "", xerrors.Errorf("unable to parse float string: %s", decimalValStr)
	}

	integerLen := precision - scale
	for i := len(arr[0]); i < integerLen; i++ {
		arr[0] = "0" + arr[0]
	}
	arr[0] = arr[0][len(arr[0])-integerLen:]
	for i := len(arr[1]); i < scale; i++ {
		arr[1] = arr[1] + "0"
	}
	arr[1] = arr[1][0:scale]
	return arr[0] + arr[1], nil
}

func DecimalToDebeziumHandlingModePrecise(decimal, decimalWithoutProvider string) (interface{}, error) {
	normalizedDecimal, err := ExponentialFloatFormToNumeric(decimal)
	if err != nil {
		return nil, xerrors.Errorf("unable to convert exponential form to numeric. dataTypeVerbose: %s, err: %w", decimalWithoutProvider, err)
	}
	putScaleToValue, _, _, err := DecimalGetPrecisionAndScale(decimalWithoutProvider)
	if err != nil {
		return nil, xerrors.Errorf("unable to determine - should we put scale to value. dataTypeVerbose: %s, err: %w", decimalWithoutProvider, err)
	}

	value, scale, err := DecimalToDebeziumPrimitivesImpl(normalizedDecimal)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract debezium primitives from number, err: %w", err)
	}

	if putScaleToValue {
		result := make(map[string]interface{})
		result["scale"] = scale
		result["value"] = value
		return result, nil
	} else {
		return value, nil
	}
}

func DecimalToDebezium(decimal, decimalWithoutProvider string, connectorParameters map[string]string) (interface{}, error) {
	decimalHandlingMode := debeziumparameters.GetDecimalHandlingMode(connectorParameters)
	switch decimalHandlingMode {
	case debeziumparameters.DecimalHandlingModePrecise:
		result, err := DecimalToDebeziumHandlingModePrecise(decimal, decimalWithoutProvider)
		if err != nil {
			return nil, xerrors.Errorf("unable to convert decimal to debezium in precise handling mode, decimal: %s, err: %w", decimal, err)
		}
		return result, nil
	case debeziumparameters.DecimalHandlingModeDouble:
		result, err := strconv.ParseFloat(decimal, 64)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse float %s, err: %w", decimal, err)
		}
		return result, nil
	case debeziumparameters.DecimalHandlingModeString:
		return decimal, nil
	default:
		return nil, xerrors.Errorf("unknown DecimalHandlingMode: %s", decimalHandlingMode)
	}
}

func fillByZeroesToAlignL(in string, lengthMustBe int) (string, error) {
	zeroesNum := lengthMustBe - len(in)
	if zeroesNum < 0 {
		return "", xerrors.Errorf("unable to align string L, which is larger than lengthMustBe, in:%s, lengthMustBe:%d", in, lengthMustBe)
	}
	return strings.Repeat("0", zeroesNum) + in, nil
}

func fillByZeroesToAlignR(in string, lengthMustBe int) (string, error) {
	zeroesNum := lengthMustBe - len(in)
	if zeroesNum < 0 {
		return "", xerrors.Errorf("unable to align string R, which is larger than lengthMustBe, in:%s, lengthMustBe:%d", in, lengthMustBe)
	}
	return in + strings.Repeat("0", zeroesNum), nil
}

func exponentialFloatFormToNumericPositivePart(in string) (string, error) {
	if strings.HasSuffix(in, "e0") {
		return in[0 : len(in)-2], nil
	}

	eIndex := strings.Index(in, "e")
	if eIndex == -1 {
		return in, nil
	}
	basePart := in[0:eIndex]
	sign := in[eIndex+1 : eIndex+2]
	offset := 2
	if sign != "-" && sign != "+" {
		sign = "+"
		offset = 1
	}
	expVal, err := strconv.Atoi(in[eIndex+offset:])
	if err != nil {
		return "", xerrors.Errorf("unknown exponential value: %s", in)
	}

	if sign == "-" {
		if basePart[0] == '.' {
			return "0." + strings.Repeat("0", expVal) + basePart[1:], nil
		} else {
			if expVal >= len(basePart) {
				result, err := fillByZeroesToAlignL(basePart, expVal)
				if err != nil {
					return "", xerrors.Errorf("unable to align L, err:%w", err)
				}
				return "0." + result, nil
			} else {
				index := len(basePart) - expVal
				return basePart[0:index] + "." + basePart[index:], nil
			}
		}
	} else if sign == "+" {
		basePart = basePart[1:]
		if expVal >= len(basePart) {
			result, err := fillByZeroesToAlignR(basePart, expVal)
			if err != nil {
				return "", xerrors.Errorf("unable to align R, err:%w", err)
			}
			return result, nil
		} else {
			index := len(basePart) - expVal
			return basePart[0:index] + "." + basePart[index:], nil
		}
	} else {
		return "", xerrors.Errorf("unknown sign: %s, string: %s", sign, in)
	}
}

func ExponentialFloatFormToNumeric(in string) (string, error) {
	if in == "" {
		return "", xerrors.New("empty string as an input is not supported")
	}

	if in[0] == '-' {
		result, err := exponentialFloatFormToNumericPositivePart(in[1:])
		if err != nil {
			return "", xerrors.Errorf("unable to convert exponentialFloatFormToNumeric, err: %w", err)
		}
		return "-" + result, nil
	} else {
		return exponentialFloatFormToNumericPositivePart(in)
	}
}

func makeNegativeNum(in *big.Int) big.Int {
	bytes := in.Bytes()
	for i := range bytes {
		bytes[i] = ^bytes[i]
	}
	var bigNum big.Int
	bigNum.SetBytes(bytes)
	var result big.Int
	result.Add(&bigNum, big.NewInt(1))
	return result
}

func containsOnly(in string, chars []rune) bool {
	for _, ch := range in {
		if !slices.Contains(chars, ch) {
			return false
		}
	}
	return true
}

func DecimalToDebeziumPrimitivesImpl(decimal string) (string, int, error) {
	decimalInt := decimal // decimalInt is `decimal` without dot ('.').
	scale := 0
	dotIndex := strings.Index(decimal, ".")
	if dotIndex != -1 {
		scale = len(decimal) - 1 - dotIndex
		decimalInt = decimalInt[0:dotIndex] + decimalInt[dotIndex+1:]
	}
	var buf []byte
	if containsOnly(decimalInt, []rune{'0', '-'}) {
		buf = []byte{0}
	} else {
		var bigNum big.Int
		_, successful := bigNum.SetString(decimalInt, 10)
		if !successful {
			return "", 0, xerrors.Errorf("unable to parse string as int: %s", decimalInt)
		}
		if bigNum.Sign() == -1 { // negative
			negativeNum := makeNegativeNum(&bigNum)
			buf = negativeNum.Bytes()
			if !isHighestBitSet(buf) { // if number is negative, but highest bit is 0 - then we need extra leading 0xFF byte
				buf = append([]byte{0xFF}, buf...)
			}
		} else { // positive
			buf = bigNum.Bytes()
			if isHighestBitSet(buf) { // if number is positive, but highest bit is 1 - then we need extra leading 0x00 byte
				buf = append([]byte{0x00}, buf...)
			}
		}
	}
	return base64.StdEncoding.EncodeToString(buf), scale, nil
}

func DecimalToDebeziumPrimitives(decimal string, connectorParameters map[string]string) (interface{}, error) {
	decimalHandlingMode := debeziumparameters.GetDecimalHandlingMode(connectorParameters)
	switch decimalHandlingMode {
	case debeziumparameters.DecimalHandlingModePrecise:
		result, _, err := DecimalToDebeziumPrimitivesImpl(decimal)
		if err != nil {
			return nil, xerrors.Errorf("unable to emit decimal debezium, val:%s, err:%w", decimal, err)
		}
		return result, nil
	case debeziumparameters.DecimalHandlingModeDouble:
		return strconv.ParseFloat(decimal, 64)
	case debeziumparameters.DecimalHandlingModeString:
		return decimal, nil
	default:
		return "", xerrors.Errorf("unknown DecimalHandlingMode: %s", decimalHandlingMode)
	}
}

var (
	pgTimestampLayout0 = "2006-01-02T15:04:05Z"
	pgTimestampLayout1 = "2006-01-02 15:04:05Z"
	pgTimestampLayout2 = "2006-01-02T15:04:05-07:00"
	pgTimestampLayout3 = "2006-01-02 15:04:05-07"
)

func ParsePgDateTimeWithTimezone(in string) (time.Time, error) {
	var result time.Time
	var err error

	if in[10] == 'T' {
		if in[len(in)-1] == 'Z' {
			result, err = time.Parse(pgTimestampLayout0, in)
		} else {
			result, err = time.Parse(pgTimestampLayout2, in)
		}
	} else {
		if in[len(in)-1] == 'Z' {
			result, err = time.Parse(pgTimestampLayout1, in)
		} else {
			result, err = time.Parse(pgTimestampLayout3, in)
		}
	}
	if err != nil {
		return time.Time{}, xerrors.Errorf("pg - timestamp with time zone - time.Parse returned error, string: %s, err: %w", in, err)
	}
	return result, nil
}

func ParsePostgresInterval(interval, intervalHandlingMode string) (interface{}, error) {
	if intervalHandlingMode == debeziumparameters.IntervalHandlingModeNumeric {
		arrStr, err := ExtractPostgresIntervalArray(interval)
		if err != nil {
			return nil, xerrors.Errorf("unable to extract postgres interval array, interval: %s, intervalHandlingMode: %s, err: %w", interval, intervalHandlingMode, err)
		}

		arrInt := make([]int64, 0)
		for _, el := range arrStr {
			if el == "" {
				arrInt = append(arrInt, 0)
				continue
			}
			i, err := strconv.ParseInt(el, 10, 64)
			if err != nil {
				return 0, xerrors.Errorf("ParseInt returned error - value: %s, interval: %s, err: %w", el, interval, err)
			}
			arrInt = append(arrInt, i)
		}

		// Internally interval values are stored as months, days, and seconds.
		// This is done because the number of days in a month varies, and a day can have 23 or 25 hours if a daylight savings time adjustment is involved.
		// The 'months' and 'days' fields are integers while the 'seconds' field can store fractions.
		// Because intervals are usually created from constant strings or timestamp subtraction, this storage method works well in most cases.
		// Functions justify_days and justify_hours are available for adjusting days and hours that overflow their normal ranges.

		years := arrInt[0]
		months := arrInt[1]
		days := arrInt[2]
		hours := arrInt[3]
		minutes := arrInt[4]
		seconds := arrInt[5]
		microseconds := arrInt[6]

		return uint64(years*31557600+months*2629800+days*86400+hours*3600+minutes*60+seconds)*1000000 + uint64(microseconds), nil
	} else {
		return nil, xerrors.Errorf("unsupported interval.handling.mode: %s", intervalHandlingMode)
	}
}

func ParseBytea(colVal interface{}, binaryHandlingMode string) (interface{}, error) {
	// pg:bytea, mysql:binary
	var bufInBase64 string

	switch t := colVal.(type) {
	case string: // restored snapshot
		bufInBase64 = t
	case []uint8: // original snapshot
		bufInBase64 = base64.StdEncoding.EncodeToString(t)
	default:
		return nil, xerrors.Errorf("unknown type of value for pg:bytea: %T", colVal)
	}

	switch binaryHandlingMode {
	case debeziumparameters.BinaryHandlingModeBytes:
		return bufInBase64, nil
	default:
		return nil, xerrors.Errorf("unsupported binary.handling.mode: %s", binaryHandlingMode)
	}
}

func UnescapeUnicode(in string) string {
	out := ""
	for len(in) > 0 {
		if in[0] == '\\' {
			if len(in) > 5 && in[1] == 'u' {
				u, err := strconv.ParseUint(in[2:6], 16, 64)
				if err != nil {
					// treat it literally
					goto literal
				}
				out += string(byte(u))
				in = in[6:]
				continue
			}
		}
	literal:
		out += in[:1]
		in = in[1:]
	}
	return out
}

func PointToDebezium(in string) (map[string]interface{}, error) {
	arr := strings.Split(in[1:len(in)-1], ",")
	if len(arr) != 2 {
		return nil, xerrors.Errorf("unknown format of point: %s", in)
	}
	x, err := strconv.ParseFloat(arr[0], 64)
	if err != nil {
		return nil, xerrors.Errorf("unable to format float x: %s, err: %w", in, err)
	}
	y, err := strconv.ParseFloat(arr[1], 64)
	if err != nil {
		return nil, xerrors.Errorf("unable to format float y: %s, err: %w", in, err)
	}

	result := make(map[string]interface{})
	result["x"] = x
	result["y"] = y
	result["wkb"] = ""
	result["srid"] = nil
	return result, nil
}

func NumRangeToDebezium(in string) (string, error) {
	arr := strings.Split(in[1:len(in)-1], ",")
	if len(arr) != 2 {
		return "", xerrors.Errorf("unknown format of numeric range: %s", in)
	}
	left, err := ExponentialFloatFormToNumeric(arr[0])
	if err != nil {
		return "", xerrors.Errorf("unable to format numeric range left border of range: %s, err: %w", in, err)
	}
	right, err := ExponentialFloatFormToNumeric(arr[1])
	if err != nil {
		return "", xerrors.Errorf("unable to format numeric range right border of range: %s, err: %w", in, err)
	}
	return "[" + left + "," + right + ")", nil
}

func UnquoteIfQuoted(in string) string {
	if len(in) == 0 {
		return ""
	}
	if in[0] == '"' && in[len(in)-1] == '"' {
		return in[1 : len(in)-1]
	} else {
		return in
	}
}

func TstZRangeQuote(colStr string) (string, error) {
	leftBracket := string(colStr[0])
	rightBracket := string(colStr[len(colStr)-1])
	colStr = colStr[1 : len(colStr)-1]
	parts := strings.Split(colStr, ",")
	l, r, err := ParsePgDateTimeWithTimezone2(UnquoteIfQuoted(parts[0]), UnquoteIfQuoted(parts[1]))
	if err != nil {
		return "", xerrors.Errorf("parsePgDateTimeWithTimezone returned error, err: %w", err)
	}
	parts[0] = "\"" + l.UTC().Format("2006-01-02 15:04:05+00") + "\""
	parts[1] = "\"" + r.UTC().Format("2006-01-02 15:04:05+00") + "\""
	resultStr := strings.Join(parts, ",")
	return leftBracket + resultStr + rightBracket, nil
}

func TstZRangeUnquote(colStr string) (string, error) {
	leftBracket := string(colStr[0])
	rightBracket := string(colStr[len(colStr)-1])
	colStr = colStr[1 : len(colStr)-1]
	parts := strings.Split(colStr, ",")
	l, r, err := ParsePgDateTimeWithTimezone2(UnquoteIfQuoted(parts[0]), UnquoteIfQuoted(parts[1]))
	if err != nil {
		return "", xerrors.Errorf("parsePgDateTimeWithTimezone returned error, err: %w", err)
	}
	parts[0] = parts[0][1 : len(parts[0])-1]
	parts[0] = l.UTC().Format("2006-01-02 15:04:05Z")
	parts[1] = parts[1][1 : len(parts[0])-1]
	parts[1] = r.UTC().Format("2006-01-02 15:04:05Z")
	resultStr := strings.Join(parts, ",")
	return leftBracket + resultStr + rightBracket, nil
}

func TSRangeUnquote(colStr string) (string, error) {
	leftBracket := string(colStr[0])
	rightBracket := string(colStr[len(colStr)-1])
	parts := strings.Split(colStr[1:len(colStr)-1], ",")
	if len(parts) != 2 {
		return "", xerrors.Errorf("unknown format of tstrange: %s", colStr)
	}
	parts[0], _ = strconv.Unquote(parts[0])
	parts[1], _ = strconv.Unquote(parts[1])
	resultStr := strings.Join(parts, ",")
	return leftBracket + resultStr + rightBracket, nil
}

func ParsePgDateTimeWithTimezone2(l, r string) (time.Time, time.Time, error) {
	lTime, err := ParsePgDateTimeWithTimezone(l)
	if err != nil {
		return time.Time{}, time.Time{}, xerrors.Errorf("pg - timestamp with time zone - time.Parse returned error, string: %s, err: %w", l, err)
	}
	rTime, err := ParsePgDateTimeWithTimezone(r)
	if err != nil {
		return time.Time{}, time.Time{}, xerrors.Errorf("pg - timestamp with time zone - time.Parse returned error, string: %s, err: %w", r, err)
	}
	return lTime, rTime, nil
}

var (
	regexHour  = *regexp.MustCompile(`(\d+):(\d+):(\d+)`)
	regexYear  = *regexp.MustCompile(`(\d+) years?`)
	regexMonth = *regexp.MustCompile(`(\d+) (?:mon|months?)`)
	regexDay   = *regexp.MustCompile(`(\d+) days?`)
)

func ExtractPostgresIntervalArray(interval string) ([]string, error) {
	result := make([]string, 7)

	arr := regexYear.FindStringSubmatch(interval)
	if arr != nil {
		result[0] = arr[1]
	}

	arr = regexMonth.FindStringSubmatch(interval)
	if arr != nil {
		result[1] = arr[1]
	}

	arr = regexDay.FindStringSubmatch(interval)
	if arr != nil {
		result[2] = arr[1]
	}

	arr = regexHour.FindStringSubmatch(interval)
	if arr != nil {
		result[3] = arr[1]
		result[4] = arr[2]
		result[5] = arr[3]
	}

	if index := strings.Index(interval, "."); index != -1 {
		result[6] = interval[index+1:]
		tmp, err := fillByZeroesToAlignR(result[6], 6)
		if err != nil {
			return nil, xerrors.Errorf("unable to align R, err:%w", err)
		}
		result[6] = tmp
	}

	return result, nil
}

var (
	timeWithoutTZ0 = "15:04:05"
	timeWithoutTZ1 = "15:04:05.0"
	timeWithoutTZ2 = "15:04:05.00"
	timeWithoutTZ3 = "15:04:05.000"
	timeWithoutTZ4 = "15:04:05.0000"
	timeWithoutTZ5 = "15:04:05.00000"
	timeWithoutTZ6 = "15:04:05.000000"
)

func ParseTimeWithoutTZ(timeStr string) (time.Time, error) {
	var layout string
	switch len(timeStr) {
	case 8:
		layout = timeWithoutTZ0
	case 10:
		layout = timeWithoutTZ1
	case 11:
		layout = timeWithoutTZ2
	case 12:
		layout = timeWithoutTZ3
	case 13:
		layout = timeWithoutTZ4
	case 14:
		layout = timeWithoutTZ5
	case 15:
		layout = timeWithoutTZ6
	default:
		return time.Time{}, xerrors.Errorf("pg - unknown format of time with time zone - %s", timeStr)
	}

	timeVal, err := time.Parse(layout, timeStr)
	if err != nil {
		return time.Time{}, xerrors.Errorf("pg - time with time zone - unknown time format: %s, err: %w", timeVal, err)
	}
	return timeVal, nil
}

var (
	timestampWithoutTZ0 = "2006-01-02T15:04:05Z"
	timestampWithoutTZ1 = "2006-01-02T15:04:05.0Z"
	timestampWithoutTZ2 = "2006-01-02T15:04:05.00Z"
	timestampWithoutTZ3 = "2006-01-02T15:04:05.000Z"
	timestampWithoutTZ4 = "2006-01-02T15:04:05.0000Z"
	timestampWithoutTZ5 = "2006-01-02T15:04:05.00000Z"
	timestampWithoutTZ6 = "2006-01-02T15:04:05.000000Z"
)

func ParseTimestamp(timeStr string) (time.Time, error) {
	var layout string
	switch len(timeStr) {
	case 20:
		layout = timestampWithoutTZ0
	case 22:
		layout = timestampWithoutTZ1
	case 23:
		layout = timestampWithoutTZ2
	case 24:
		layout = timestampWithoutTZ3
	case 25:
		layout = timestampWithoutTZ4
	case 26:
		layout = timestampWithoutTZ5
	case 27:
		layout = timestampWithoutTZ6
	default:
		return time.Time{}, xerrors.Errorf("pg - unknown format of timestamp - %s", timeStr)
	}

	timeVal, err := time.Parse(layout, timeStr)
	if err != nil {
		return time.Time{}, xerrors.Errorf("pg - time with time zone - unknown time format: %s, err: %w", timeVal, err)
	}
	return timeVal, nil
}

func FormatTime(inputTime time.Time, precision int) string {
	result := inputTime.UTC().Format("2006-01-02T15:04:05")
	microSecondsStr := fmt.Sprintf("%06d", inputTime.UTC().UnixMicro()%1000000)
	microSecondsStr = microSecondsStr[0:precision]
	microSecondsStr = strings.TrimRight(microSecondsStr, "0")
	if len(microSecondsStr) == 0 {
		return result + "Z"
	} else {
		return result + "." + microSecondsStr + "Z"
	}
}

func BufToChangeItemsBits(in []byte) string {
	encodedStr := hex.EncodeToString(in)
	result := ""
	for _, ch := range encodedStr {
		switch ch {
		case '0':
			result += "0000"
		case '1':
			result += "0001"
		case '2':
			result += "0010"
		case '3':
			result += "0011"
		case '4':
			result += "0100"
		case '5':
			result += "0101"
		case '6':
			result += "0110"
		case '7':
			result += "0111"
		case '8':
			result += "1000"
		case '9':
			result += "1001"
		case 'a':
			result += "1010"
		case 'b':
			result += "1011"
		case 'c':
			result += "1100"
		case 'd':
			result += "1101"
		case 'e':
			result += "1110"
		case 'f':
			result += "1111"
		}
	}
	return result
}

var (
	yearMS   = int64(31557600000000)
	monthMS  = int64(2629800000000)
	dayMS    = int64(3600 * 24 * 1000 * 1000)
	hourMS   = int64(3600 * 1000 * 1000)
	minuteMS = int64(60 * 1000 * 1000)
	secondMS = int64(1000 * 1000)
)

func EmitPostgresInterval(val int64) string {
	y := val / yearMS
	least := val - y*yearMS
	month := least / monthMS
	least = least - month*monthMS
	d := least / dayMS
	least = least - d*dayMS
	h := least / hourMS
	least = least - h*hourMS
	min := least / minuteMS
	least = least - min*minuteMS
	s := least / secondMS
	ms := least - s*secondMS

	makeEndOfBlock := func(in string, val int64) string {
		if val%10 != 1 {
			return in + "s "
		} else {
			return in + " "
		}
	}

	result := ""
	if y != 0 {
		result += fmt.Sprintf("%d year", y)
		result = makeEndOfBlock(result, y)
	}
	if month != 0 {
		result += fmt.Sprintf("%d month", month)
		result = makeEndOfBlock(result, month)
	}
	if d != 0 {
		result += fmt.Sprintf("%d day", d)
		result = makeEndOfBlock(result, d)
	}
	if !(h == 0 && min == 0 && s == 0 && ms == 0) {
		result += fmt.Sprintf("%02d:%02d:%02d.%06d", h, min, s, ms)
		result = makeEndOfBlock(result, d)
	}
	return strings.TrimSuffix(result, " ")
}

func ParseMysqlBit(colVal interface{}, _ string) (interface{}, error) {
	switch t := colVal.(type) {
	case []uint8: // original snapshot
		t = ReverseBytesArr(t)
		return base64.StdEncoding.EncodeToString(t), nil
	case string: // restored snapshot
		resultBuf, err := base64.StdEncoding.DecodeString(t)
		if err != nil {
			return nil, xerrors.Errorf("unable to decode base64: %s, err: %w", t, err)
		}
		result := make([]byte, len(resultBuf))
		for i := 0; i < len(resultBuf); i++ {
			result[i] = resultBuf[len(resultBuf)-1-i]
		}
		return base64.StdEncoding.EncodeToString(result), nil
	default:
		return nil, xerrors.Errorf("unknown type of value for mysql:bit: %T", colVal)
	}
}

func NumericToExponentialFloatForm(in string) (string, error) {
	_, scale, err := DecimalToDebeziumPrimitivesImpl(in)
	if err != nil {
		return "", xerrors.Errorf("unable to convert float form, in: %s, err: %w", in, err)
	}
	result := strings.ReplaceAll(in, ".", "")
	for {
		if strings.HasPrefix(result, "0") {
			result = strings.TrimPrefix(result, "0")
		} else {
			break
		}
	}
	sign := "+"
	if scale > 0 {
		sign = "-"
	} else if scale == 0 {
		sign = ""
	}

	if sign == "" {
		return result, nil
	} else {
		return result + "e" + sign + strconv.Itoa(scale), nil
	}
}

func NumRangeFromDebezium(in string) (string, error) {
	arr := strings.Split(in[1:len(in)-1], ",")
	if len(arr) != 2 {
		return "", xerrors.Errorf("unknown format of numeric range: %s", in)
	}
	left, err := NumericToExponentialFloatForm(arr[0])
	if err != nil {
		return "", xerrors.Errorf("unable to format numeric range left border of range: %s, err: %w", in, err)
	}
	right, err := NumericToExponentialFloatForm(arr[1])
	if err != nil {
		return "", xerrors.Errorf("unable to format numeric range right border of range: %s, err: %w", in, err)
	}
	return "[" + left + "," + right + ")", nil
}

func isHighestBitSet(in []byte) bool {
	return in[0]&0x80 != 0
}

func Base64ToNumeric(based64buf string, scale int) (string, error) {
	resultBuf, err := base64.StdEncoding.DecodeString(based64buf)
	if err != nil {
		return "", xerrors.Errorf("unable to decode base64: %s, err: %w", based64buf, err)
	}
	var resultStr string
	sign := ""
	bigNum := new(big.Int)
	bigNum.SetBytes(resultBuf)
	if !isHighestBitSet(resultBuf) { // positive
		resultStr = bigNum.String()
	} else {
		negativeNum := makeNegativeNum(bigNum)
		sign = "-"
		resultStr = negativeNum.String()
	}
	if resultStr == "0" {
		return resultStr, nil
	}
	if scale != 0 {
		if scale > len(resultStr) { // case '0.01' for example - then resultStr=='1' & scale=2
			resultStr = strings.Repeat("0", scale-len(resultStr)+1) + resultStr
		}
		resultStr = resultStr[0:len(resultStr)-scale] + "." + resultStr[len(resultStr)-scale:]
	}
	return sign + resultStr, nil
}

func DateToInt32(v time.Time) int32 {
	return int32(v.Unix() / (3600 * 24))
}

func DatetimeToSecs(vv time.Time) int64 {
	return vv.UnixMicro() / 1000
}

func DatetimeToMicrosecs(vv time.Time) int64 {
	return vv.UnixMicro()
}

func TimeFromDate(in int64) time.Time {
	return time.Unix(in*3600*24, 0).UTC()
}

func TimeFromDatetime(in int64) time.Time {
	return time.Unix(0, in*1000*1000).UTC()
}

func TimeFromTimestamp(in int64) time.Time {
	return time.Unix(0, in*1000).UTC()
}

func UnwrapMysqlEnumsAndSets(in string) (string, error) {
	str := in
	result := ""
	for {
		str = str[1:]
		idx := strings.Index(str, "'")
		if idx == -1 {
			return "", xerrors.Errorf("unable to find right quote in string: %s", in)
		}
		result += str[0:idx] + ","
		if idx+2 >= len(str) {
			break
		}
		str = str[idx+2:]
	}
	return result[0 : len(result)-1], nil
}

func ShrinkMysqlBit(colVal interface{}, colType string) ([]uint8, error) {
	buf := colVal.([]uint8)

	sizeStr := colType[10:]
	idx := strings.Index(sizeStr, ")")
	sizeStr = sizeStr[0:idx]
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract size from mysql:bit, type: %s", colType)
	}

	div := size / 8
	mod := size % 8
	if mod != 0 {
		div++
	}
	return buf[len(buf)-div:], nil
}

var reParametrizedType = regexp.MustCompile(`^.*\((\d)\).*`)

// MysqlFitBinaryLength - used for types: "mysql:binary", "mysql:varbinary", "mysql:longblob", "mysql:mediumblob", "mysql:blob", "mysql:tinyblob"
func MysqlFitBinaryLength(colType string, colVal interface{}) (interface{}, error) {
	reMatch := reParametrizedType.FindStringSubmatch(colType)
	if len(reMatch) == 2 {
		length, _ := strconv.Atoi(reMatch[1])
		switch t := colVal.(type) {
		case []uint8:
			return append(t, make([]uint8, length-len(t))...), nil
		case string:
			buf, err := base64.StdEncoding.DecodeString(t)
			if err != nil {
				return nil, xerrors.Errorf("unable to decode base64, string: %s, err: %w", t, err)
			}
			return append(buf, make([]uint8, length-len(buf))...), nil
		default:
			return nil, xerrors.Errorf("unknown type of mysql binary type, colType=%s, type(colVal)=%T, colVal=%v", colType, colVal, colVal)
		}
	}
	return colVal, nil
}

func ExtractParameter(originalType string) (int, error) {
	reMatch := reParametrizedType.FindStringSubmatch(originalType)
	if len(reMatch) == 2 {
		return strconv.Atoi(reMatch[1])
	}
	return 0, xerrors.Errorf("unable to find parameter, originalType: %s", originalType)
}

func MakeFractionSecondSuffix(fraction int64, precision int) string {
	if precision == 0 {
		return ""
	} else {
		fractionStr := fmt.Sprintf("%06d", fraction)[0:precision]
		return "." + fractionStr
	}
}

const fileOffset = 1_000_000_000_000

func LSNToFileAndPos(lsn uint64) (string, uint64) {
	return fmt.Sprintf("mysql-log.%06d", lsn/fileOffset), lsn % fileOffset
}

func SprintfDebeziumTime(in time.Time) string {
	ns := in.Nanosecond()
	nsStr := fmt.Sprintf("%09d", ns)
	nsStr = strings.TrimRight(nsStr, "0")
	if len(nsStr) == 0 {
		return in.UTC().Format("2006-01-02T15:04:05Z")
	} else {
		return in.UTC().Format("2006-01-02T15:04:05") + "." + nsStr + "Z"
	}
}

func OriginalTypeWithoutProvider(originalType string) string {
	index := strings.Index(originalType, ":")
	if index == -1 { // impossible case
		return ""
	}
	return originalType[index+1:]
}

var (
	pgTimeWithoutTimeZoneParam = *regexp.MustCompile(`pg:time\((\d)\) without time zone`)
	pgNumeric                  = *regexp.MustCompile(`pg:numeric\(\d+,\d+\)`)
)

func PgTimeWithoutTimeZonePrecision(originalType string) int {
	if originalType == "pg:time without time zone" {
		return 0
	} else {
		arr := pgTimeWithoutTimeZoneParam.FindStringSubmatch(originalType)
		if len(arr) <= 1 {
			return 0
		}
		val, _ := strconv.Atoi(arr[1])
		return val
	}
}

func IsPgNumeric(originalType string) bool {
	if originalType == "pg:numeric" {
		return true
	}
	arr := pgNumeric.FindStringSubmatch(originalType)
	return len(arr) != 0
}

func UnknownTypeToString(in interface{}) (string, error) {
	switch t := in.(type) {
	case string:
		return t, nil
	default:
		result, err := util.JSONMarshalUnescape(in)
		if err != nil {
			return "", xerrors.Errorf("unable to marshal unknown type to string, err: %w", err)
		}
		return string(result), nil
	}
}
