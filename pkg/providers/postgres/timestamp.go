//go:build !disable_postgres_provider

package postgres

import (
	"database/sql/driver"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/pkg/providers/postgres/sqltimestamp"
	"github.com/transferia/transferia/pkg/util"
)

// TimeZoneParameterStatusKey is the identifier of the PostgreSQL connection property containing time zone
const TimeZoneParameterStatusKey string = "TimeZone"

type Timestamp struct {
	pgtype.Timestamp

	location *time.Location
}

var _ TextDecoderAndValuerWithHomo = (*Timestamp)(nil)

// NewTimestamp constructs a TIMESTAMP WITHOUT TIME ZONE representation which supports BC years
//
// TODO: this type must become significantly simpler after https://st.yandex-team.ru/TM-5127 is done
func NewTimestamp(tz *time.Location) *Timestamp {
	return &Timestamp{
		Timestamp: *(new(pgtype.Timestamp)),

		location: tz,
	}
}

func (t *Timestamp) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if err := t.Timestamp.DecodeText(ci, src); err != nil {
		tim, errF := sqltimestamp.Parse(string(src))
		infmod := isTimestampInfinite(string(src))
		if errF != nil && infmod != pgtype.None {
			return util.Errors{err, errF}
		}
		t.Timestamp = pgtype.Timestamp{Time: tim, Status: pgtype.Present, InfinityModifier: infmod}
	}

	if t.Timestamp.Status != pgtype.Present || t.Timestamp.InfinityModifier != pgtype.None {
		return nil
	}

	// https://st.yandex-team.ru/TM-5092 - timestamps without time zone must be parsed in the source database's time zone
	parsed := t.Timestamp.Time
	t.Timestamp.Time = time.Date(actualYear(parsed), parsed.Month(), parsed.Day(), parsed.Hour(), parsed.Minute(), parsed.Second(), parsed.Nanosecond(), t.location)

	return nil
}

func (t *Timestamp) Value() (driver.Value, error) {
	return t.Timestamp.Value()
}

func (t *Timestamp) HomoValue() any {
	switch t.Timestamp.Status {
	case pgtype.Null:
		return nil
	case pgtype.Undefined:
		return nil
	}
	return t.Timestamp
}

func actualYear(t time.Time) int {
	result := t.Year() // this value is always positive, even for BC (negative) years
	if t.Before(util.BeforeChristEnding(t.Location())) {
		result = -result
	}
	return result
}

// MinusToBC checks if the given string starts with a minus and if so, trims it and adds a "BC" suffix
func MinusToBC(v string) string {
	if strings.HasPrefix(v, "-") {
		return strings.TrimPrefix(v, "-") + " BC"
	}
	return v
}

func isTimestampInfinite(timestamp string) pgtype.InfinityModifier {
	switch timestamp {
	case "infinity":
		return pgtype.Infinity
	case "-infinity":
		return pgtype.NegativeInfinity
	default:
		return pgtype.None
	}
}
