//go:build !disable_postgres_provider

package tasks

import (
	"reflect"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

func tryComparePg(lOriginalIsPG bool, rOriginalIsPG bool, lVal interface{}, rVal interface{}, lS string, rS string, lSchema abstract.ColSchema, rSchema abstract.ColSchema) (bool, error) {
	var lPGType, rPGType string
	if lOriginalIsPG {
		lPGType = postgres.ClearOriginalType(lSchema.OriginalType)
	}
	if rOriginalIsPG {
		rPGType = postgres.ClearOriginalType(rSchema.OriginalType)
	}

	if anyOfAmong(lPGType, rPGType, "BYTEA") {
		lBytes, lErr := extractBytes(lVal)
		rBytes, rErr := extractBytes(rVal)
		if lErr == nil && rErr == nil {
			return reflect.DeepEqual(lBytes, rBytes), nil
		}
	}
	if anyOfAmong(lPGType, rPGType, "LSEG") {
		return compareSegments(lS, rS), nil
	}
	if anyOfAmong(lPGType, rPGType, "BOX") {
		if equal, err := compareGeometry(lS, rS, parseBox); err != nil {
			return false, xerrors.Errorf("failed to compare pg:box: %w", err)
		} else {
			return equal, nil
		}
	}
	if anyOfAmong(lPGType, rPGType, "CIRCLE") {
		if equal, err := compareGeometry(lS, rS, parseCircle); err != nil {
			return false, xerrors.Errorf("failed to compare pg:circle: %w", err)
		} else {
			return equal, nil
		}
	}
	if anyOfAmong(lPGType, rPGType, "POLYGON") {
		if equal, err := compareGeometry(lS, rS, parsePolygon); err != nil {
			return false, xerrors.Errorf("failed to compare pg:polygon: %w", err)
		} else {
			return equal, nil
		}
	}

	return false, nil
}

func tryCompareTemporalsPg(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema) (mustReturn bool, comparable bool, result bool, err error) {
	lS, lSOk := lVal.(string)
	rS, rSOk := rVal.(string)
	castsToString := lSOk && rSOk
	switch {
	case postgres.IsPgTypeTimeWithTimeZone(lSchema.OriginalType) && postgres.IsPgTypeTimeWithTimeZone(rSchema.OriginalType):
		{
			if !castsToString {
				return true, false, false, nil
			}
			lT, err := postgres.TimeWithTimeZoneToTime(lS)
			if err != nil {
				return true, false, false, xerrors.Errorf("failed to represent %q as time.Time: %w", lS, err)
			}
			rT, err := postgres.TimeWithTimeZoneToTime(rS)
			if err != nil {
				return true, false, false, xerrors.Errorf("failed to represent %q as time.Time: %w", rS, err)
			}
			return true, true, lT.Equal(rT), nil
		}
	case postgres.IsPgTypeTimestampWithTimeZone(lSchema.OriginalType) && postgres.IsPgTypeTimestampWithTimeZone(rSchema.OriginalType):
		{
			lT, lOk := lVal.(time.Time)
			rT, rOk := rVal.(time.Time)
			if !lOk || !rOk {
				return true, false, false, nil
			}
			return true, true, lT.Format(pgTimestampWithoutTZFormat) == rT.Format(pgTimestampWithoutTZFormat), nil
		}
	}
	return false, false, false, nil
}
