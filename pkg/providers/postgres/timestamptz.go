//go:build !disable_postgres_provider

package postgres

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/pkg/providers/postgres/sqltimestamp"
	"github.com/transferia/transferia/pkg/util"
)

type Timestamptz struct {
	pgtype.Timestamptz
}

var _ TextDecoderAndValuerWithHomo = (*Timestamptz)(nil)

// NewTimestamptz constructs a TIMESTAMP WITH TIME ZONE representation which supports BC years
//
// TODO: remove this when https://st.yandex-team.ru/TM-5127 is done
func NewTimestamptz() *Timestamptz {
	return &Timestamptz{
		Timestamptz: *new(pgtype.Timestamptz),
	}
}

func (t *Timestamptz) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if err := t.Timestamptz.DecodeText(ci, src); err != nil {
		tim, errF := sqltimestamp.Parse(string(src))
		infmod := isTimestampInfinite(string(src))
		if errF != nil && infmod != pgtype.None {
			return util.Errors{err, errF}
		}
		t.Timestamptz = pgtype.Timestamptz{Time: tim, Status: pgtype.Present, InfinityModifier: infmod}
	}

	return nil
}

func (t *Timestamptz) Value() (driver.Value, error) {
	return t.Timestamptz.Value()
}

func (t *Timestamptz) HomoValue() any {
	switch t.Timestamptz.Status {
	case pgtype.Null:
		return nil
	case pgtype.Undefined:
		return nil
	}
	return t.Timestamptz
}
