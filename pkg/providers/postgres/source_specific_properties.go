//go:build !disable_postgres_provider

package postgres

import (
	"github.com/transferia/transferia/pkg/abstract"
)

const (
	EnumAllValues    = abstract.PropertyKey("pg:enum_all_values")
	DatabaseTimeZone = abstract.PropertyKey("pg:database_timezone")
)

func GetPropertyEnumAllValues(in *abstract.ColSchema) []string {
	if in == nil {
		return nil
	}
	if val, ok := in.Properties[EnumAllValues]; ok {
		return val.([]string)
	}
	return nil
}
