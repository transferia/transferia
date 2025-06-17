//go:build !disable_mysql_provider

package mysql

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestNotMasterErrorWrapping(t *testing.T) {
	abstract.CheckOpaqueErrorWrapping(t, "struct", func(err error) bool {
		return xerrors.Is(err, *new(NotMasterError))
	}, func(err error) error {
		return *new(NotMasterError)
	})
	abstract.CheckOpaqueErrorWrapping(t, "pointer", func(err error) bool {
		return xerrors.Is(err, *new(NotMasterError))
	}, func(err error) error {
		return new(NotMasterError)
	})
}

func TestTimezoneOffset(t *testing.T) {
	tests := []struct {
		name        string
		location    string
		timeToCheck time.Time
		expectedUTC string
	}{
		{"UTC", "UTC", time.Now(), "+00:00"},
		{"Empty location (defaults to UTC)", "", time.Now(), "+00:00"},
		{"Europe/Moscow", "Europe/Moscow", time.Now(), "+03:00"},
		// winter / summer times
		{"America/Los_Angeles Summer", "America/Los_Angeles", time.Date(2023, time.July, 1, 0, 0, 0, 0, time.UTC), "-07:00"},
		{"America/Los_Angeles Winter", "America/Los_Angeles", time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC), "-08:00"},
		{"Pacific/Marquesas", "Pacific/Marquesas", time.Now(), "-09:30"},
		{"Pacific/Kiritimati", "Pacific/Kiritimati", time.Now(), "+14:00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc, err := time.LoadLocation(tt.location)
			require.NoError(t, err)

			offset := timezoneOffsetAtTime(loc, tt.timeToCheck)
			require.Equal(t, tt.expectedUTC, offset, "Unexpected timezone offset for %s at %v", tt.location, tt.timeToCheck)
		})
	}
}

// timezoneOffsetAtTime returns the UTC offset for a specific time in a given location.
func timezoneOffsetAtTime(loc *time.Location, t time.Time) string {
	_, offsetSeconds := t.In(loc).Zone()
	offsetHours := offsetSeconds / 3600
	offsetMinutes := (offsetSeconds % 3600) / 60
	return fmt.Sprintf("%+03d:%02d", offsetHours, int(math.Abs(float64(offsetMinutes))))
}
