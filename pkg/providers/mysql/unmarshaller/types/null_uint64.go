package types

import (
	"database/sql"
	"database/sql/driver"

	"golang.org/x/xerrors"
)

type NullUint64 struct {
	UInt64 uint64
	Valid  bool // Valid is true if Int64 is not NULL
}

var _ driver.Valuer = (*NullUint64)(nil)
var _ sql.Scanner = (*NullUint64)(nil)

func (n *NullUint64) Scan(value interface{}) error {
	n.UInt64 = uint64(0)
	n.Valid = false

	if value == nil {
		n.Valid = false
		return nil
	}

	switch t := value.(type) {
	case uint64:
		n.UInt64 = t
		n.Valid = true
	default:
		return xerrors.Errorf("unable to scan, expected: uint64, got: %T", value)
	}

	return nil
}

func (n *NullUint64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.UInt64, nil
}
