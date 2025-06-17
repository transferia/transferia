//go:build !disable_postgres_provider

package sqltimestamp

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

var InfiniteTimestampError = xerrors.NewSentinel("infinity is unparseable by the common timestamp parser")

// Parse converts a text representation of an PostgreSQL-formatted date, timestamp, or timestamptz into time.Time.
// In contrast with pgtype parsers, this parser supports BC years.
//
// TODO: remove this when https://st.yandex-team.ru/TM-5127 is done
func Parse(timestamp string) (time.Time, error) {
	switch timestamp {
	case "":
		return time.Time{}, xerrors.New("input is empty")
	case "infinity":
		return time.Time{}, xerrors.Errorf("input is '+infinity': %w", InfiniteTimestampError)
	case "-infinity":
		return time.Time{}, xerrors.Errorf("input is '-infinity': %w", InfiniteTimestampError)
	}

	globalParts := strings.Split(timestamp, " ")

	// parse year-month-day

	dateParts := strings.Split(globalParts[0], "-")
	if len(dateParts) != 3 {
		return time.Time{}, xerrors.Errorf("Invalid date part '%v' of timestamp '%v'", globalParts[0], timestamp)
	}

	year, err := strconv.Atoi(dateParts[0])
	if err != nil {
		return time.Time{}, xerrors.Errorf("Invalid year part '%v' of timestamp '%v'", dateParts[0], timestamp)
	}

	month, err := strconv.Atoi(dateParts[1])
	if err != nil {
		return time.Time{}, xerrors.Errorf("Invalid month part '%v' of timestamp '%v'", dateParts[1], timestamp)
	}

	day, err := strconv.Atoi(dateParts[2])
	if err != nil {
		return time.Time{}, xerrors.Errorf("Invalid day part '%v' of timestamp '%v'", dateParts[2], timestamp)
	}

	if len(globalParts) == 1 {
		return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
	}
	if len(globalParts) == 2 && globalParts[1] == "BC" {
		return time.Date(-year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
	}

	// parse hour:minute:second.millisecond(+/-)offset

	hasOffset := false
	hasNegativeOffset := false
	splitTimeFunc := func(r rune) bool {
		if r == '+' || r == '-' {
			hasOffset = true
			if r == '-' {
				hasNegativeOffset = true
			}
			return true
		}
		return false
	}

	timeAndOffsetParts := strings.FieldsFunc(globalParts[1], splitTimeFunc)

	timeParts := strings.Split(timeAndOffsetParts[0], ":")
	if len(timeParts) != 3 {
		return time.Time{}, xerrors.Errorf("Invalid time part '%v' of timestamp '%v'", globalParts[1], timestamp)
	}

	hour, err := strconv.Atoi(timeParts[0])
	if err != nil {
		return time.Time{}, xerrors.Errorf("Invalid hour part '%v' of timestamp '%v'", timeParts[0], timestamp)
	}

	minute, err := strconv.Atoi(timeParts[1])
	if err != nil {
		return time.Time{}, xerrors.Errorf("Invalid minute part '%v' of timestamp '%v'", timeParts[1], timestamp)
	}

	secondParts := strings.Split(timeParts[2], ".")
	if len(secondParts) > 2 {
		return time.Time{}, xerrors.Errorf("Invalid time part '%v' of timestamp '%v'", globalParts[1], timestamp)
	}

	second, err := strconv.Atoi(secondParts[0])
	if err != nil {
		return time.Time{}, xerrors.Errorf("Invalid second part '%v' of timestamp '%v'", secondParts[0], timestamp)
	}

	var nanosecond int
	if len(secondParts) == 2 {
		if len(secondParts[1]) > 6 {
			return time.Time{}, xerrors.Errorf("Invalid microsecond part '%v' of timestamp '%v'", secondParts[1], timestamp)
		}

		nanosecond, err = strconv.Atoi(secondParts[1])
		if err != nil {
			return time.Time{}, xerrors.Errorf("Invalid microsecond part '%v' of timestamp '%v'", secondParts[1], timestamp)
		}
		nanosecond *= int(math.Pow10(6 - len(secondParts[1]))) // convert to microseconds
		nanosecond *= 1000                                     // convert to nanoseconds
	}

	location := time.UTC
	if hasOffset {
		if len(timeAndOffsetParts) != 2 {
			return time.Time{}, xerrors.Errorf("Timestamp '%v' contains invalid location offset", timestamp)
		}

		offsetParts := strings.Split(timeAndOffsetParts[1], ":")
		offset := 0 // in seconds

		offsetHour, err := strconv.Atoi(offsetParts[0])
		if err != nil {
			return time.Time{}, xerrors.Errorf("Invalid offset hour part '%v' of timestamp '%v'", offsetParts[0], timestamp)
		}
		offset += offsetHour * 60 * 60

		if len(offsetParts) > 1 {
			offsetMinute, err := strconv.Atoi(offsetParts[1])
			if err != nil {
				return time.Time{}, xerrors.Errorf("Invalid offset minute part '%v' of timestamp '%v'", offsetParts[1], timestamp)
			}
			offset += offsetMinute * 60
		}

		if len(offsetParts) > 2 {
			offsetSecond, err := strconv.Atoi(offsetParts[2])
			if err != nil {
				return time.Time{}, xerrors.Errorf("Invalid offset second part '%v' of timestamp '%v'", offsetParts[2], timestamp)
			}
			offset += offsetSecond
		}

		if len(offsetParts) > 3 {
			return time.Time{}, xerrors.Errorf("Invalid location offset part '%v' of timestamp '%v'", timeAndOffsetParts[1], timestamp)
		}

		if hasNegativeOffset {
			offset *= -1
		}

		location = time.FixedZone("", offset)
	}

	if len(globalParts) == 2 {
		return time.Date(year, time.Month(month), day, hour, minute, second, nanosecond, location), nil
	}
	if len(globalParts) == 3 && globalParts[2] == "BC" {
		return time.Date(-year, time.Month(month), day, hour, minute, second, nanosecond, location), nil
	}

	return time.Time{}, xerrors.Errorf("Timestamp '%v' contains unknown parts", timestamp)
}
