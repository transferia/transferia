//go:build !disable_postgres_provider

package postgres

import (
	"time"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// dateWithEquallyDistancedTimezones is a date when there was no daylight saving time in any time zone in the world.
// The date should not actually matter because PostgreSQL should output timezone offsets as numbers. In this case, all timezone offsets in the world are the same independent of their date.
const dateWithEquallyDistancedTimezones = "1500-05-15"

// TimeWithTimeZoneToTime converts the given (valid) PostgreSQL TIME WITH TIME ZONE into a time.Time whose time zone and time are set to valid values
// All resulting time.Times are guaranteed to be properly comparable among themselves. This implies they are all based on the same date.
func TimeWithTimeZoneToTime(val string) (time.Time, error) {
	val = dateWithEquallyDistancedTimezones + " " + val
	ts := new(pgtype.Timestamptz)
	if err := ts.DecodeText(nil, []byte(val)); err != nil {
		return time.Time{}, xerrors.Errorf("failed to decode TIME WITH TIME ZONE: %w", err)
	}
	if ts.Status != pgtype.Present || ts.InfinityModifier != pgtype.None {
		return time.Time{}, xerrors.Errorf("TIME WITH TIME ZONE decoded into a nil or an infinity")
	}
	return ts.Time, nil
}
