package postgres

import (
	"github.com/transferria/transferria/pkg/abstract"
)

const EnumAllValues = abstract.PropertyKey("pg:enum_all_values")
const DatabaseTimeZone = abstract.PropertyKey("pg:database_timezone")

func GetPropertyEnumAllValues(in *abstract.ColSchema) []string {
	if in == nil {
		return nil
	}
	if val, ok := in.Properties[EnumAllValues]; ok {
		return val.([]string)
	}
	return nil
}
