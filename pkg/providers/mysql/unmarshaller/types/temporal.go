//go:build !disable_mysql_provider

package types

import (
	"database/sql"
	"database/sql/driver"
	"regexp"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres/sqltimestamp"
)

// Temporal enables to scan any temporal value with a date from []byte
type Temporal struct {
	location *time.Location
	isNull   bool
	raw      string
}

var (
	_ driver.Valuer       = (*Temporal)(nil)
	_ sql.Scanner         = (*Temporal)(nil)
	_ abstract.HomoValuer = (*Temporal)(nil)
)

func NewTemporal() *Temporal {
	return &Temporal{
		location: nil,
		isNull:   true,
		raw:      "",
	}
}

func NewTemporalInLocation(location *time.Location) *Temporal {
	return &Temporal{
		location: location,
		isNull:   true,
		raw:      "",
	}
}

func (t *Temporal) Scan(src any) error { // Implements sql.Scanner
	t.raw = ""
	t.isNull = false

	switch input := src.(type) {
	case []byte:
		t.raw = string(input)
	case nil:
		t.isNull = true
	default:
		return xerrors.Errorf("expected input of type []byte or nil, got %T", src)
	}
	return nil
}

func (t *Temporal) Value() (driver.Value, error) {
	if t.isNull {
		return nil, nil
	}

	// MySQL may store invalid dates `0000-00-00`. These are converted into `nil` in Transfer, as they are not representable in the strict type system. See also https://github.com/go-sql-driver/mysql/issues/741
	if isMySQLZeroDate(t.raw) {
		return nil, nil
	}

	result, err := sqltimestamp.Parse(t.raw)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse a non-nil temporal value '%s': %w", t.raw, err)
	}

	if t.location != nil && result.Location().String() != t.location.String() {
		result = time.Date(
			result.Year(), result.Month(), result.Day(),
			result.Hour(), result.Minute(), result.Second(), result.Nanosecond(),
			t.location)
	}

	return result, nil
}

func isMySQLZeroDate(v string) bool {
	return mySQLZeroDateRe.MatchString(v)
}

var mySQLZeroDateRe *regexp.Regexp = regexp.MustCompile(`^[0-9]{4}-00-00`)

func (t *Temporal) HomoValue() any {
	if t.isNull {
		return nil
	}
	return t.raw
}
