package helpers

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

func PgDebeziumIgnoreTemporalAccuracyForArraysComparator(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema, intoArray bool) (comparable bool, result bool, err error) {
	if !intoArray {
		return false, false, nil
	}

	lS, lSOk := lVal.(string)
	rS, rSOk := rVal.(string)
	castsToString := lSOk && rSOk

	if postgres.IsPgTypeTimeWithTimeZone(lSchema.OriginalType) && postgres.IsPgTypeTimeWithTimeZone(rSchema.OriginalType) {
		if !castsToString {
			return false, false, nil
		}
		lT, err := postgres.TimeWithTimeZoneToTime(lS)
		if err != nil {
			return false, false, xerrors.Errorf("failed to represent %q as time.Time: %w", lS, err)
		}
		rT, err := postgres.TimeWithTimeZoneToTime(rS)
		if err != nil {
			return false, false, xerrors.Errorf("failed to represent %q as time.Time: %w", rS, err)
		}
		return true, lT.UTC().Format("15:04:05") == rT.UTC().Format("15:04:05"), nil
	}

	if postgres.IsPgTypeTimeWithoutTimeZone(lSchema.OriginalType) && postgres.IsPgTypeTimeWithoutTimeZone(rSchema.OriginalType) {
		if !castsToString {
			return false, false, nil
		}
		return true, TimeWithPrecision(lS, 3) == TimeWithPrecision(rS, 3), nil
	}

	return false, false, nil
}

// TimeWithPrecision takes the time in format `01:02:03[.123456]` and returns it with the given precision
func TimeWithPrecision(t string, precision int) string {
	withoutFractions := t[:8]
	fractions := t[8:]
	if len(fractions) > 0 {
		// remove the leading dot
		fractions = fractions[1:]
	}
	if len(fractions) > 0 {
		fractions = fractions[:precision]
	}
	if len(fractions) > 0 {
		return withoutFractions + "." + fractions
	}
	return withoutFractions
}
