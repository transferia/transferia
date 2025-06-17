//go:build !disable_mysql_provider

package types

import (
	"bytes"
	"database/sql"
	"database/sql/driver"

	"github.com/transferia/transferia/pkg/util/jsonx"
	"golang.org/x/xerrors"
)

type JSON struct {
	value interface{}
}

var (
	_ driver.Valuer = (*JSON)(nil)
	_ sql.Scanner   = (*JSON)(nil)
)

func (j *JSON) Scan(src any) error { // Implements sql.Scanner
	j.value = nil

	switch input := src.(type) {
	case []byte: // In the driver JSON is represented with sql.RawBytes, not a string: https://github.com/transferia/transferia/arcadia/vendor/github.com/go-sql-driver/mysql/fields.go?rev=10013250#L181-183
		res, err := jsonx.NewValueDecoder(jsonx.NewDefaultDecoder(bytes.NewBuffer(input))).Decode()
		if err != nil {
			return xerrors.Errorf("cannot unmarshal json: %w", err)
		}
		j.value = res
	case nil:
		j.value = nil
	default:
		return xerrors.Errorf("expected input of type []byte or nil for JSON column, got %T", src)
	}
	return nil
}

func (j *JSON) Value() (driver.Value, error) {
	return j.value, nil
}
