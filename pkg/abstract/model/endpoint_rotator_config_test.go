package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
)

// test runner
func TestRotatorConfig(t *testing.T) {
	t.Run("ScenarioTesting", scenarioTesting)
	t.Run("NilWorkaround", nilWorkaround) // temporary test
	t.Run("Validate", validate)
	t.Run("GetMonthPartitionedTestLight", getMonthPartitionedTestLight)
	t.Run("GetMonthPartitionedTestHeavy", getMonthPartitionedTestHeavy)
	t.Run("OffsetDateTest", offsetDateTest)
	t.Run("GetPartitionBin", getPartitionBin)
	t.Run("DateBinToPartitionNameTest", dateBinToPartitionNameTest)
	t.Run("AnnotateWithTimeFromColumnTest", annotateWithTimeFromColumnTest)
	t.Run("Next", nextTest)
	t.Run("TestBaseTime", testBaseTime)
	t.Run("TestTimeParsing", testTimeParsing)
}

// some month utility
var monthIds = map[time.Month]int{
	time.January: 0, time.February: 1, time.March: 2, time.April: 3, time.May: 4,
	time.June: 5, time.July: 6, time.August: 7, time.September: 8, time.October: 9, time.November: 10, time.December: 11,
}

var monthList = []time.Month{
	time.January, time.February, time.March, time.April, time.May,
	time.June, time.July, time.August, time.September, time.October, time.November, time.December,
}

// scenario tests
func scenarioTesting(t *testing.T) {
	// this tests based on real user scenarios
	t.Parallel()
	t.Run("DTSUPPORT-693", scenarioTestingDTSUPPORT693)
}

func scenarioTestingDTSUPPORT693(t *testing.T) {
	t.Parallel()
	// TODO(kry127) should refactor and make arbitrary rotation: TM-2195
	rotator := RotatorConfig{PartType: RotatorPartHour, PartSize: 1, KeepPartCount: 6, TableNameTemplate: "{{partition}}"}

	var rotationTables []string
	timestamp := time.Now()

	addProcedure := func(tableList []string, currentTime time.Time) []string {
		return append(tableList, rotator.AnnotateWithTime("whatever", currentTime))
	}
	dropProcedure := func(tableList []string, currentTime time.Time) []string {
		var filteredTableList []string
		baseTime := rotator.baseTime(currentTime)
		for _, table := range tableList {
			tableTime, err := rotator.ParseTime(table)
			require.NoError(t, err)
			if !tableTime.Before(baseTime) {
				filteredTableList = append(filteredTableList, table)
			}
		}
		return filteredTableList
	}

	// fill up first timestamps
	for i := 0; i < rotator.KeepPartCount; i++ {
		rotationTables = addProcedure(rotationTables, timestamp)
		timestamp = timestamp.Add(time.Hour)
	}
	require.Equal(t, rotator.KeepPartCount, len(rotationTables), "Check that rotations correctly initiated")
	require.Equal(t, rotationTables, dropProcedure(rotationTables, timestamp.Add(-time.Hour)), "Should not drop initial amount of rotations")
	for N := 0; N < 10; N++ {
		rotationTables = addProcedure(rotationTables, timestamp)
		rotationTables = dropProcedure(rotationTables, timestamp)
		timestamp = timestamp.Add(time.Hour)
		require.Equal(t, rotator.KeepPartCount, len(rotationTables), "Check that there is always window of six tables")
	}
	for N := 0; N < 10; N++ {
		rotationTables = dropProcedure(rotationTables, timestamp)
		rotationTables = addProcedure(rotationTables, timestamp)
		timestamp = timestamp.Add(time.Hour)
		require.Equal(t, rotator.KeepPartCount, len(rotationTables), "Check that there is always window of six tables")
	}

}

// tests
func nilWorkaround(t *testing.T) {
	// TODO delete this test when workaround is not necessary anymore
	t.Parallel()
	var nilPtrRotator *RotatorConfig = nil
	cfg := nilPtrRotator.NilWorkaround()
	require.NoError(t, cfg.Validate())

	cfg = &RotatorConfig{KeepPartCount: 0, PartType: RotatorPartHour, PartSize: 0, TimeColumn: ""}
	require.Error(t, cfg.Validate(), "PartSize and KeepPartCount should not be zero")
	require.NoError(t, cfg.NilWorkaround().Validate(), "PartSize=0 and KeepPartCount=0 fixed with workaround is valid")

	cfg = &RotatorConfig{KeepPartCount: 1, PartType: RotatorPartHour, PartSize: 0, TimeColumn: ""}
	require.Error(t, cfg.Validate(), "PartSize should not be zero")
	require.Error(t, cfg.NilWorkaround().Validate(), "NilWorkaround is not working on partially zeroified structures")

	cfg = &RotatorConfig{KeepPartCount: 0, PartType: RotatorPartHour, PartSize: 1, TimeColumn: ""}
	require.Error(t, cfg.Validate(), "PartSize should not be zero")
	require.Error(t, cfg.NilWorkaround().Validate(), "NilWorkaround is not working on partially zeroified structures")
}

func validate(t *testing.T) {
	t.Parallel()
	var nilPtrRotator *RotatorConfig = nil
	require.NoError(t, nilPtrRotator.Validate(), "Nil pointer receiver is valid")
	require.NoError(t, (&RotatorConfig{KeepPartCount: 1, PartType: RotatorPartHour, PartSize: 1, TimeColumn: ""}).Validate(), "KeepPartCount=1 and PartSize=1 is valid")
	require.NoError(t, (&RotatorConfig{KeepPartCount: 3, PartType: RotatorPartHour, PartSize: 25, TimeColumn: ""}).Validate(), "overflow of PartSize more than hours in day is valid")
	require.NoError(t, (&RotatorConfig{KeepPartCount: 24, PartType: RotatorPartDay, PartSize: 32, TimeColumn: ""}).Validate(), "overflow of PartSize more than days in month is valid")
	require.NoError(t, (&RotatorConfig{KeepPartCount: 2, PartType: RotatorPartMonth, PartSize: 13, TimeColumn: ""}).Validate(), "overflow of PartSize more than months in year is valid")

	require.Error(t, (&RotatorConfig{KeepPartCount: 1, PartType: RotatorPartHour, PartSize: 0, TimeColumn: ""}).Validate(), "PartSize should be positive")
	require.Error(t, (&RotatorConfig{KeepPartCount: 1, PartType: RotatorPartHour, PartSize: -1, TimeColumn: ""}).Validate(), "PartSize should be positive")
	require.Error(t, (&RotatorConfig{KeepPartCount: 0, PartType: RotatorPartHour, PartSize: 1, TimeColumn: ""}).Validate(), "KeepPartCount should be positive")
	require.Error(t, (&RotatorConfig{KeepPartCount: 1, PartType: "kek", PartSize: 1, TimeColumn: ""}).Validate(), "wrong enum value of PartType")
}

func getMonthPartitionedTestLight(t *testing.T) {
	t.Parallel()

	rc2 := RotatorConfig{PartSize: 2}
	require.Equal(t, time.January, rc2.getMonthPartitioned(time.January))
	require.Equal(t, time.January, rc2.getMonthPartitioned(time.February))
	require.Equal(t, time.March, rc2.getMonthPartitioned(time.March))
	require.Equal(t, time.March, rc2.getMonthPartitioned(time.April))
	require.Equal(t, time.May, rc2.getMonthPartitioned(time.May))
	require.Equal(t, time.May, rc2.getMonthPartitioned(time.June))
	require.Equal(t, time.July, rc2.getMonthPartitioned(time.July))
	require.Equal(t, time.July, rc2.getMonthPartitioned(time.August))
	require.Equal(t, time.September, rc2.getMonthPartitioned(time.September))
	require.Equal(t, time.September, rc2.getMonthPartitioned(time.October))
	require.Equal(t, time.November, rc2.getMonthPartitioned(time.November))
	require.Equal(t, time.November, rc2.getMonthPartitioned(time.December))

	rc5 := RotatorConfig{PartSize: 5}
	require.Equal(t, time.January, rc5.getMonthPartitioned(time.January))
	require.Equal(t, time.January, rc5.getMonthPartitioned(time.February))
	require.Equal(t, time.January, rc5.getMonthPartitioned(time.March))
	require.Equal(t, time.January, rc5.getMonthPartitioned(time.April))
	require.Equal(t, time.January, rc5.getMonthPartitioned(time.May))
	require.Equal(t, time.June, rc5.getMonthPartitioned(time.June))
	require.Equal(t, time.June, rc5.getMonthPartitioned(time.July))
	require.Equal(t, time.June, rc5.getMonthPartitioned(time.August))
	require.Equal(t, time.June, rc5.getMonthPartitioned(time.September))
	require.Equal(t, time.June, rc5.getMonthPartitioned(time.October))
	require.Equal(t, time.November, rc5.getMonthPartitioned(time.November))
	require.Equal(t, time.November, rc5.getMonthPartitioned(time.December))

	rc13 := RotatorConfig{PartSize: 13}
	for month := range monthIds {
		require.Equal(t, time.January, rc13.getMonthPartitioned(month), "PartSize overflow of months per year check")
	}

	var nilPtrRotator *RotatorConfig = nil
	for month := range monthIds {
		require.Equal(t, month, nilPtrRotator.getMonthPartitioned(month), "nil receiver preserves month rotation")
	}
}

func getMonthPartitionedTestHeavy(t *testing.T) {
	t.Parallel()
	rc := RotatorConfig{}
	for partSize := 1; partSize <= len(monthIds); partSize++ {
		rc.PartSize = partSize
		for month, i := range monthIds {
			require.Equal(t, monthList[i-(i%partSize)], rc.getMonthPartitioned(month))
		}
	}

}

func offsetDateTest(t *testing.T) {
	t.Setenv("TZ", "Europe/Moscow") // this test is timezone aware
	t.Run("Hours", offsetDateTestHours)
	t.Run("Days", offsetDateTestDays)
	//t.Run("MonthHeavy", offsetDateTestMonthHeavy) // TODO(@kry127) temporary switched off
	t.Run("NilReceiver", offsetDateTestNilReceiver)
}

func offsetDateTestHours(t *testing.T) {
	rcHours := RotatorConfig{KeepPartCount: 0, PartType: RotatorPartHour, PartSize: 1, TimeColumn: ""}
	rcHoursTimestamp := time.Now()

	for partSize := 1; partSize <= 5; partSize++ {
		rcHours.PartSize = partSize
		require.Equal(t, time.Duration(0), rcHours.offsetDate(rcHoursTimestamp, 0).Sub(rcHoursTimestamp), "offset 0 for hour is identity with PartSize=%d", partSize)
		for offset := 1; offset < 15; offset++ {
			hOffset := partSize * offset
			require.Equal(t, time.Duration(hOffset)*time.Hour, rcHours.offsetDate(rcHoursTimestamp, offset).Sub(rcHoursTimestamp), "offset %d for hour is %d hours with PartSize=%d", offset, hOffset, partSize)
		}
		for offset := 1; offset < 15; offset++ {
			hOffset := partSize * offset
			require.Equal(t, time.Duration(hOffset)*time.Hour, rcHoursTimestamp.Sub(rcHours.offsetDate(rcHoursTimestamp, -offset)), "offset -%d for hour is %d hours with PartSize=%d", offset, hOffset, partSize)
		}
	}
}

func offsetDateTestDays(t *testing.T) {
	rcDays := RotatorConfig{KeepPartCount: 0, PartType: RotatorPartDay, PartSize: 1, TimeColumn: ""}
	rcDaysTimestamp := time.Now()

	for partSize := 1; partSize <= 5; partSize++ {
		rcDays.PartSize = partSize
		require.Equal(t, time.Duration(0), rcDays.offsetDate(rcDaysTimestamp, 0).Sub(rcDaysTimestamp), "offset 0 for day is identity with PartSize=%d", partSize)
		for offset := 1; offset < 15; offset++ {
			hOffset := 24 * partSize * offset
			require.Equal(t, time.Duration(hOffset)*time.Hour, rcDays.offsetDate(rcDaysTimestamp, offset).Sub(rcDaysTimestamp), "offset %d for day is %d hours with PartSize=%d", offset, hOffset, partSize)
		}
		for offset := 1; offset < 15; offset++ {
			hOffset := 24 * partSize * offset
			require.Equal(t, time.Duration(hOffset)*time.Hour, rcDaysTimestamp.Sub(rcDays.offsetDate(rcDaysTimestamp, -offset)), "offset -%d for day is %d hours with PartSize=%d", offset, hOffset, partSize)
		}
	}
}

func offsetDateTestNilReceiver(t *testing.T) {
	var nilPtrRotator *RotatorConfig = nil
	for offset := 1; offset <= 5; offset++ {
		ts := time.Now()
		require.Equal(t, ts, nilPtrRotator.offsetDate(ts, offset), "nil receiver preserves month rotation")
	}
}

func isLeap(year int) bool {
	if year%400 == 0 {
		return true // 3. but every four hundredth year is and exception again
	}
	if year%100 == 0 {
		return false // 2. except for every hundredth year
	}
	if year%4 == 0 {
		return true // 1. every fourth year is leap
	}
	return false
}

func countDaysForYearAcc(year, month, offset int, acc int64) int64 {
	var (
		monthDayCount     = []int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
		monthDayCountLeap = []int{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	)

	switch {
	case offset > 0:
		dayDiff := 0
		if isLeap(year) {
			dayDiff = monthDayCountLeap[month]
		} else {
			dayDiff = monthDayCount[month]
		}
		nextMonth := month + 1
		nextYear := year
		if nextMonth == len(monthList) {
			nextMonth = 0
			nextYear++
		}
		return countDaysForYearAcc(nextYear, nextMonth, offset-1, acc+int64(dayDiff))
	case offset < 0:
		prevMonth := month - 1
		prevYear := year
		if prevMonth < 0 {
			prevMonth = len(monthList) - 1
			prevYear--
		}

		dayDiff := 0
		if isLeap(prevYear) {
			dayDiff = monthDayCountLeap[prevMonth]
		} else {
			dayDiff = monthDayCount[prevMonth]
		}
		return countDaysForYearAcc(prevYear, prevMonth, offset-1, acc-int64(dayDiff))
	default:
		return acc
	}
}

func countDaysForYear(year, month, offset int) int64 {
	return countDaysForYearAcc(year, month, offset, 0)
}

func TestOffsetDateTestMonthHeavy(t *testing.T) {
	checkYear := func(t *testing.T, year, partSize int) {
		t.Skip()
		rcMonths := RotatorConfig{KeepPartCount: 0, PartType: RotatorPartMonth, PartSize: partSize, TimeColumn: ""}
		nowTimestamp := time.Now()
		for offset := 1; offset < 15; offset++ {
			for monthID, month := range monthList {
				// NOTE UTC parameter in tests! This test will not work for RotationTZ timezone with +1 and -1 correction hours
				ts := time.Date(year, month, nowTimestamp.Day(), nowTimestamp.Hour(), nowTimestamp.Minute(), nowTimestamp.Second(), nowTimestamp.Nanosecond(), time.UTC)
				offTimestamp := rcMonths.offsetDate(ts, offset)

				expectedHoursOffset := 24 * countDaysForYear(year, monthID, offset*partSize)
				require.Equal(t, time.Duration(expectedHoursOffset)*time.Hour, offTimestamp.Sub(ts), "offset=%d for year=%d, month=%v should be %d hours with PartSize=%d", offset, year, month, expectedHoursOffset, partSize)

			}
		}
	}

	// check some juicy years
	// leap years
	t.Run("check year 2000 part size 1", func(t *testing.T) { checkYear(t, 2000, 1) })
	t.Run("check year 2000 part size 2", func(t *testing.T) { checkYear(t, 2000, 2) })
	t.Run("check year 2004 part size 3", func(t *testing.T) { checkYear(t, 2004, 3) })
	t.Run("check year 2005 part size 4", func(t *testing.T) { checkYear(t, 2005, 4) })
	t.Run("check year 2024 part size 1", func(t *testing.T) { checkYear(t, 2024, 1) })
	t.Run("check year 2028 part size 1", func(t *testing.T) { checkYear(t, 2028, 1) })
	t.Run("check year 2400 part size 1", func(t *testing.T) { checkYear(t, 2005, 1) })
	t.Run("check year 2400 part size 2", func(t *testing.T) { checkYear(t, 2024, 2) })
	t.Run("check year 2400 part size 3", func(t *testing.T) { checkYear(t, 2028, 3) })

	// not leap years
	t.Run("check year 2021 part size 1", func(t *testing.T) { checkYear(t, 2021, 1) })
	t.Run("check year 2021 part size 2", func(t *testing.T) { checkYear(t, 2021, 2) })
	t.Run("check year 2021 part size 3", func(t *testing.T) { checkYear(t, 2021, 3) })
	t.Run("check year 2021 part size 5", func(t *testing.T) { checkYear(t, 2021, 5) })
	t.Run("check year 2021 part size 6", func(t *testing.T) { checkYear(t, 2021, 6) })
	t.Run("check year 2021 part size 7", func(t *testing.T) { checkYear(t, 2021, 7) })
	t.Run("check year 2100 part size 1", func(t *testing.T) { checkYear(t, 2100, 1) })
	t.Run("check year 2100 part size 5", func(t *testing.T) { checkYear(t, 2100, 5) })
	t.Run("check year 2100 part size 12", func(t *testing.T) { checkYear(t, 2100, 12) })
}

func getPartitionBin(t *testing.T) {
	t.Parallel()
	var rotatorNil RotatorConfig
	ts := time.Now()
	require.Equal(t, dateBin(ts), rotatorNil.getPartitionBin(ts), "nil receiver should preserve argument in getPartitionBin")
}

func dateBinToPartitionNameTest(t *testing.T) {
	t.Parallel()
	date := time.Date(2006, time.January, 2, 15, 04, 05, 0, RotationTZ)
	hourRotator := RotatorConfig{PartType: RotatorPartHour}
	dayRotator := RotatorConfig{PartType: RotatorPartDay}
	monthRotator := RotatorConfig{PartType: RotatorPartMonth}
	var nilRotator *RotatorConfig = nil
	require.Equal(t, HourFormat, string(hourRotator.dateBinToPartitionName(dateBin(date))))
	require.Equal(t, DayFormat, string(dayRotator.dateBinToPartitionName(dateBin(date))))
	require.Equal(t, MonthFormat, string(monthRotator.dateBinToPartitionName(dateBin(date))))
	require.Equal(t, MonthFormat, string(nilRotator.dateBinToPartitionName(dateBin(date))))
}

func annotateWithTimeFromColumnTest(t *testing.T) {
	t.Parallel()
	t.Run("WithoutFormat", annotateWithTimeFromColumnTestWithoutFormat)
	t.Run("WithFormat", annotateWithTimeFromColumnTestWithFormat)
	t.Run("NoTimeColumnFound", annotateWithTimeFromColumnTestNoTimeColumnInRawData)
	t.Run("NoTimeColumnInConfig", annotateWithTimeFromColumnTestNoTimeColumnInConfig)
	t.Run("NilReceiver", annotateWithTimeFromColumnTestNilReceiver)
}

func annotateWithTimeFromColumnTestWithoutFormat(t *testing.T) {
	t.Parallel()
	t.Run("Hours", annotateWithTimeFromColumnTestWithoutFormatHours)
	t.Run("Days", annotateWithTimeFromColumnTestWithoutFormatDays)
	t.Run("Months", annotateWithTimeFromColumnTestWithoutFormatMonths)
}

func annotateWithTimeFromColumnTestWithFormat(t *testing.T) {
	t.Parallel()
	t.Run("Hours", annotateWithTimeFromColumnTestWithTemplateHours)
	t.Run("Days", annotateWithTimeFromColumnTestWithFormatDays)
	t.Run("Months", annotateWithTimeFromColumnTestWithFormatMonths)
}

func annotateWithTimeFromColumnTestWithoutFormatHours(t *testing.T) {
	t.Parallel()
	timeColumn := "tc"
	partType := RotatorPartHour
	var rc RotatorConfig
	var rotation string
	var timestamp time.Time

	rc = RotatorConfig{PartType: partType, TimeColumn: timeColumn}
	timestamp = time.Date(1987, time.July, 14, 23, 21, 45, 0, RotationTZ)
	changeItem := abstract.ChangeItem{
		ColumnNames:  []string{timeColumn},
		ColumnValues: []interface{}{timestamp},
	}

	for partSize, expected := range map[int]string{
		1:  "tbl/1987-07-14T23:00:00",
		2:  "tbl/1987-07-14T22:00:00",
		3:  "tbl/1987-07-14T21:00:00",
		4:  "tbl/1987-07-14T20:00:00",
		5:  "tbl/1987-07-14T20:00:00",
		6:  "tbl/1987-07-14T18:00:00",
		7:  "tbl/1987-07-14T21:00:00",
		8:  "tbl/1987-07-14T16:00:00",
		9:  "tbl/1987-07-14T18:00:00",
		10: "tbl/1987-07-14T20:00:00",
		11: "tbl/1987-07-14T22:00:00",
		12: "tbl/1987-07-14T12:00:00",
		13: "tbl/1987-07-14T13:00:00",
		14: "tbl/1987-07-14T14:00:00",
		15: "tbl/1987-07-14T15:00:00",
		16: "tbl/1987-07-14T16:00:00",
		17: "tbl/1987-07-14T17:00:00",
		18: "tbl/1987-07-14T18:00:00",
		19: "tbl/1987-07-14T19:00:00",
		20: "tbl/1987-07-14T20:00:00",
		21: "tbl/1987-07-14T21:00:00",
		22: "tbl/1987-07-14T22:00:00",
		23: "tbl/1987-07-14T23:00:00",
		24: "tbl/1987-07-14T00:00:00",
		25: "tbl/1987-07-14T00:00:00",
	} {
		rc.PartSize = partSize
		rotation = rc.AnnotateWithTimeFromColumn("tbl", changeItem)
		require.Equal(t, expected, rotation, "partSize=%d", partSize)
	}
}

func annotateWithTimeFromColumnTestWithoutFormatDays(t *testing.T) {
	t.Parallel()
	timeColumn := "time col"
	partType := RotatorPartDay
	var rc RotatorConfig
	var rotation string
	var timestamp time.Time

	rc = RotatorConfig{PartType: partType, TimeColumn: timeColumn}
	timestamp = time.Date(1997, time.May, 31, 8, 12, 03, 1337, RotationTZ)
	changeItem := abstract.ChangeItem{
		ColumnNames:  []string{timeColumn},
		ColumnValues: []interface{}{timestamp},
	}

	for partSize, expected := range map[int]string{
		1:  "table/1997-05-31",
		2:  "table/1997-05-31",
		3:  "table/1997-05-31",
		4:  "table/1997-05-29",
		5:  "table/1997-05-31",
		6:  "table/1997-05-31",
		7:  "table/1997-05-29",
		8:  "table/1997-05-25",
		9:  "table/1997-05-28",
		10: "table/1997-05-31",
		11: "table/1997-05-23",
		12: "table/1997-05-25",
		13: "table/1997-05-27",
		14: "table/1997-05-29",
		15: "table/1997-05-31",
		16: "table/1997-05-17",
		17: "table/1997-05-18",
		18: "table/1997-05-19",
		19: "table/1997-05-20",
		20: "table/1997-05-21",
		21: "table/1997-05-22",
		22: "table/1997-05-23",
		23: "table/1997-05-24",
		24: "table/1997-05-25",
		25: "table/1997-05-26",
		26: "table/1997-05-27",
		27: "table/1997-05-28",
		28: "table/1997-05-29",
		29: "table/1997-05-30",
		30: "table/1997-05-31",
		31: "table/1997-05-01",
		32: "table/1997-05-01",
		33: "table/1997-05-01",
	} {
		rc.PartSize = partSize
		rotation = rc.AnnotateWithTimeFromColumn("table", changeItem)
		require.Equal(t, expected, rotation, "partSize=%d", partSize)
	}
}

func annotateWithTimeFromColumnTestWithoutFormatMonths(t *testing.T) {
	t.Parallel()
	timeColumn := "rotationalEveryMonth"
	partType := RotatorPartMonth
	var rc RotatorConfig
	var rotation string
	var timestamp time.Time

	rc = RotatorConfig{PartType: partType, TimeColumn: timeColumn}
	timestamp = time.Date(2002, time.December, 13, 16, 47, 00, 691832, RotationTZ)
	changeItem := abstract.ChangeItem{
		ColumnNames:  []string{timeColumn},
		ColumnValues: []interface{}{timestamp},
	}

	for partSize, expected := range map[int]string{
		1:  "annual/2002-12",
		2:  "annual/2002-11",
		3:  "annual/2002-10",
		4:  "annual/2002-09",
		5:  "annual/2002-11",
		6:  "annual/2002-07",
		7:  "annual/2002-08",
		8:  "annual/2002-09",
		9:  "annual/2002-10",
		10: "annual/2002-11",
		11: "annual/2002-12",
		12: "annual/2002-01",
		13: "annual/2002-01",
	} {
		rc.PartSize = partSize
		rotation = rc.AnnotateWithTimeFromColumn("annual", changeItem)
		require.Equal(t, expected, rotation, "partSize=%d", partSize)
	}
}

func annotateWithTimeFromColumnTestWithTemplateHours(t *testing.T) {
	t.Parallel()
	timeColumn := "formatted_hour_rotator"
	partType := RotatorPartHour
	format := "{{name}}_{{partition}}_{{name}}"
	var rc RotatorConfig
	var rotation string
	var timestamp time.Time

	rc = RotatorConfig{PartType: partType, TimeColumn: timeColumn, TableNameTemplate: format}
	timestamp = time.Date(1430, time.January, 10, 23, 8, 30, 143721739, RotationTZ)
	changeItem := abstract.ChangeItem{
		ColumnNames:  []string{timeColumn},
		ColumnValues: []interface{}{timestamp},
	}

	for partSize, expected := range map[int]string{
		1:  "fine_1430-01-10T23:00:00_fine",
		2:  "fine_1430-01-10T22:00:00_fine",
		3:  "fine_1430-01-10T21:00:00_fine",
		4:  "fine_1430-01-10T20:00:00_fine",
		5:  "fine_1430-01-10T20:00:00_fine",
		6:  "fine_1430-01-10T18:00:00_fine",
		7:  "fine_1430-01-10T21:00:00_fine",
		8:  "fine_1430-01-10T16:00:00_fine",
		9:  "fine_1430-01-10T18:00:00_fine",
		10: "fine_1430-01-10T20:00:00_fine",
		11: "fine_1430-01-10T22:00:00_fine",
		12: "fine_1430-01-10T12:00:00_fine",
		13: "fine_1430-01-10T13:00:00_fine",
		14: "fine_1430-01-10T14:00:00_fine",
		15: "fine_1430-01-10T15:00:00_fine",
		16: "fine_1430-01-10T16:00:00_fine",
		17: "fine_1430-01-10T17:00:00_fine",
		18: "fine_1430-01-10T18:00:00_fine",
		19: "fine_1430-01-10T19:00:00_fine",
		20: "fine_1430-01-10T20:00:00_fine",
		21: "fine_1430-01-10T21:00:00_fine",
		22: "fine_1430-01-10T22:00:00_fine",
		23: "fine_1430-01-10T23:00:00_fine",
		24: "fine_1430-01-10T00:00:00_fine",
		25: "fine_1430-01-10T00:00:00_fine",
	} {
		rc.PartSize = partSize
		rotation = rc.AnnotateWithTimeFromColumn("fine", changeItem)
		require.Equal(t, expected, rotation, "partSize=%d", partSize)
	}
}

func annotateWithTimeFromColumnTestWithFormatDays(t *testing.T) {
	t.Parallel()
	timeColumn := "formatted_dayz"
	partType := RotatorPartDay
	format := "{{partition}}/{{name}}"
	var rc RotatorConfig
	var rotation string
	var timestamp time.Time

	rc = RotatorConfig{PartType: partType, TimeColumn: timeColumn, TableNameTemplate: format}
	timestamp = time.Date(2400, time.February, 29, 3, 56, 28, 7005194, RotationTZ)
	changeItem := abstract.ChangeItem{
		ColumnNames:  []string{timeColumn},
		ColumnValues: []interface{}{timestamp},
	}

	for partSize, expected := range map[int]string{
		1:  "2400-02-29/table",
		2:  "2400-02-29/table",
		3:  "2400-02-28/table",
		4:  "2400-02-29/table",
		5:  "2400-02-26/table",
		6:  "2400-02-25/table",
		7:  "2400-02-29/table",
		8:  "2400-02-25/table",
		9:  "2400-02-28/table",
		10: "2400-02-21/table",
		11: "2400-02-23/table",
		12: "2400-02-25/table",
		13: "2400-02-27/table",
		14: "2400-02-29/table",
		15: "2400-02-16/table",
		16: "2400-02-17/table",
		17: "2400-02-18/table",
		18: "2400-02-19/table",
		19: "2400-02-20/table",
		20: "2400-02-21/table",
		21: "2400-02-22/table",
		22: "2400-02-23/table",
		23: "2400-02-24/table",
		24: "2400-02-25/table",
		25: "2400-02-26/table",
		26: "2400-02-27/table",
		27: "2400-02-28/table",
		28: "2400-02-29/table",
		29: "2400-02-01/table",
		30: "2400-02-01/table",
		31: "2400-02-01/table",
		32: "2400-02-01/table",
		33: "2400-02-01/table",
	} {
		rc.PartSize = partSize
		rotation = rc.AnnotateWithTimeFromColumn("table", changeItem)
		require.Equal(t, expected, rotation, "partSize=%d", partSize)
	}
}

func annotateWithTimeFromColumnTestWithFormatMonths(t *testing.T) {
	t.Parallel()
	timeColumn := "rotationalEveryMonth"
	partType := RotatorPartMonth
	format := "{{name}}/{{partition}}"
	var rc RotatorConfig
	var rotation string
	var timestamp time.Time

	rc = RotatorConfig{PartType: partType, TimeColumn: timeColumn, TableNameTemplate: format}
	timestamp = time.Date(2021, time.July, 1, 19, 27, 52, 7005194, RotationTZ)
	changeItem := abstract.ChangeItem{
		ColumnNames:  []string{timeColumn},
		ColumnValues: []interface{}{timestamp},
	}

	for partSize, expected := range map[int]string{
		1:  "current_table/2021-07",
		2:  "current_table/2021-07",
		3:  "current_table/2021-07",
		4:  "current_table/2021-05",
		5:  "current_table/2021-06",
		6:  "current_table/2021-07",
		7:  "current_table/2021-01",
		8:  "current_table/2021-01",
		9:  "current_table/2021-01",
		10: "current_table/2021-01",
		11: "current_table/2021-01",
		12: "current_table/2021-01",
		13: "current_table/2021-01",
	} {
		rc.PartSize = partSize
		rotation = rc.AnnotateWithTimeFromColumn("current_table", changeItem)
		require.Equal(t, expected, rotation, "partSize=%d", partSize)
	}
}

func annotateWithTimeFromColumnTestNoTimeColumnInRawData(t *testing.T) {
	t.Parallel()
	timeColumn := "time_column"
	rc := RotatorConfig{PartType: RotatorPartHour, TimeColumn: timeColumn, PartSize: 1}
	changeItem := abstract.ChangeItem{
		ColumnNames:  []string{"top", "kek", "che", "bu", "rek", "rime_column"},
		ColumnValues: []interface{}{"yehal", "greka", "che", "rez", "reku", time.Date(1909, time.May, 3, 12, 17, 32, 0, RotationTZ)},
	}
	annotate1 := rc.Annotate("Roga-I-Kopyta")
	annotate2 := rc.AnnotateWithTimeFromColumn("Roga-I-Kopyta", changeItem)
	// if annotate1 was executed not in the same month, they can be different
	if annotate1 != annotate2 {
		// if this code will be executed approx. one hour, there are other problems to solve
		annotate1 = rc.Annotate("Roga-I-Kopyta")
	}
	require.Equal(t, annotate1, annotate2, "When time column is absent in raw data, current date and time should be used by default")
}

func annotateWithTimeFromColumnTestNoTimeColumnInConfig(t *testing.T) {
	t.Parallel()
	rc := RotatorConfig{PartType: RotatorPartHour, PartSize: 1}
	changeItem := abstract.ChangeItem{
		ColumnNames:  []string{"time_column"},
		ColumnValues: []interface{}{time.Date(1927, time.August, 15, 19, 12, 38, 0, RotationTZ)},
	}
	annotate1 := rc.Annotate("KostyaLososInc")
	annotate2 := rc.AnnotateWithTimeFromColumn("KostyaLososInc", changeItem)
	// if annotate1 was executed not in the same month, they can be different
	if annotate1 != annotate2 {
		// if this code will be executed approx. one hour, there are other problems to solve
		annotate1 = rc.Annotate("KostyaLososInc")
	}
	require.Equal(t, annotate1, annotate2, "When time column is absent in configuration, current date and time should be used by default")
}

func annotateWithTimeFromColumnTestNilReceiver(t *testing.T) {
	t.Parallel()
	var nilRotator *RotatorConfig
	changeItem := abstract.ChangeItem{
		ColumnNames:  []string{"time_column"},
		ColumnValues: []interface{}{time.Date(1927, time.April, 16, 13, 22, 5, 0, RotationTZ)},
	}

	annotate1 := nilRotator.Annotate("ElonMusksTable")
	annotate2 := nilRotator.AnnotateWithTimeFromColumn("ElonMusksTable", changeItem)
	if annotate1 != annotate2 {
		annotate1 = nilRotator.Annotate("ElonMusksTable")
	}
	require.Equal(t, annotate1, annotate2, "nil reciver should use current date for annotation as default method does")
}

func nextTest(t *testing.T) {
	t.Parallel()
	rotator := RotatorConfig{PartType: RotatorPartHour, PartSize: 1, TableNameTemplate: "{{partition}}"} // use partition only

	annotate1 := rotator.Next("hello_table")
	annotate2 := rotator.Annotate("hello_table")
	if annotate1 == annotate2 {
		// maybe annotate 2 hops to next hour
		annotate1 = rotator.Next("hello_table")
	}

	require.NotEqual(t, annotate1, annotate2, "nil reciver should use current date for annotation as default method does")
	ts1, err := time.Parse(HourFormat, annotate1)
	require.NoError(t, err)
	ts2, err := time.Parse(HourFormat, annotate2)
	require.NoError(t, err)
	require.Equal(t, ts1.Sub(ts2), time.Hour, "Next should produce one hour window")
}

func testBaseTime(t *testing.T) {
	t.Parallel()

	var nilRotatior *RotatorConfig = nil
	require.True(t, nilRotatior.BaseTime().Unix() <= 0, "nil receiver should produce zero or negative unix base time (i.e. earlier than 1970 year)")

	rotator := RotatorConfig{PartType: RotatorPartHour, PartSize: 2, TableNameTemplate: "{{partition}}", KeepPartCount: 3} // use partition only

	bt := rotator.BaseTime()
	ts1 := time.Now()
	acceptableDiff := time.Second
	require.True(t, (6*time.Hour-ts1.Sub(bt)) < acceptableDiff, "base time should give 'PartSize * KeepPartCount' delay")
}

func testTimeParsing(t *testing.T) {
	t.Parallel()
	t.Run("Hours", testTimeParsingHours)
	t.Run("Days", testTimeParsingDays)
	t.Run("Months", testTimeParsingMonths)
}

func testTimeParsingHours(t *testing.T) {
	t.Parallel()
	rotator := RotatorConfig{PartType: RotatorPartHour}

	for rotationalTime, expected := range map[string]time.Time{
		"1575-01-14T17:17:29": time.Date(1575, time.January, 14, 17, 17, 29, 0, RotationTZ),
		"2021-08-06T01:29:43": time.Date(2021, time.August, 06, 01, 29, 43, 0, RotationTZ),
		"2004-02-29T06:11:58": time.Date(2004, time.February, 29, 06, 11, 58, 0, RotationTZ),
	} {
		actual, err := rotator.ParseTime(rotationalTime)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}

func testTimeParsingDays(t *testing.T) {
	t.Parallel()
	rotator := RotatorConfig{PartType: RotatorPartDay}

	for rotationTime, expected := range map[string]time.Time{
		"1575-01-14": time.Date(1575, time.January, 14, 0, 0, 0, 0, RotationTZ),
		"2021-08-06": time.Date(2021, time.August, 06, 0, 0, 0, 0, RotationTZ),
		"2004-02-29": time.Date(2004, time.February, 29, 0, 0, 0, 0, RotationTZ),
	} {
		actual, err := rotator.ParseTime(rotationTime)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}

func testTimeParsingMonths(t *testing.T) {
	t.Parallel()
	rotator := RotatorConfig{PartType: RotatorPartMonth}

	for rotationTime, expected := range map[string]time.Time{
		"1575-01": time.Date(1575, time.January, 1, 0, 0, 0, 0, RotationTZ),
		"2021-08": time.Date(2021, time.August, 1, 0, 0, 0, 0, RotationTZ),
		"2004-02": time.Date(2004, time.February, 1, 0, 0, 0, 0, RotationTZ),
	} {
		actual, err := rotator.ParseTime(rotationTime)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}
