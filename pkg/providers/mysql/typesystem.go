//go:build !disable_mysql_provider

package mysql

import (
	"regexp"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[schema.Type][]string{
		schema.TypeInt64:   {"BIGINT"},
		schema.TypeInt32:   {"INT", "MEDIUMINT"},
		schema.TypeInt16:   {"SMALLINT"},
		schema.TypeInt8:    {"TINYINT"},
		schema.TypeUint64:  {"BIGINT UNSIGNED"},
		schema.TypeUint32:  {"INT UNSIGNED", "MEDIUMINT UNSIGNED"},
		schema.TypeUint16:  {"SMALLINT UNSIGNED"},
		schema.TypeUint8:   {"TINYINT UNSIGNED"},
		schema.TypeFloat32: {},
		schema.TypeFloat64: {"DECIMAL", "DECIMAL UNSIGNED", "DOUBLE", "FLOAT", "FLOAT UNSIGNED"},
		schema.TypeBytes: {
			"TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY", "BIT",
			"GEOMETRY", "GEOMCOLLECTION", "POINT", "MULTIPOINT", "LINESTRING", "MULTILINESTRING", "POLYGON", "MULTIPOLYGON",
			typesystem.RestPlaceholder,
		},
		schema.TypeString:    {"TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "VARCHAR", "CHAR", "TIME", "YEAR", "ENUM", "SET"},
		schema.TypeBoolean:   {},
		schema.TypeDate:      {"DATE"},
		schema.TypeDatetime:  {},
		schema.TypeTimestamp: {"DATETIME", "TIMESTAMP"},
		schema.TypeAny:       {"JSON"},
	})
	typesystem.TargetRule(ProviderType, map[schema.Type]string{
		schema.TypeInt64:     "BIGINT",
		schema.TypeInt32:     "INT",
		schema.TypeInt16:     "SMALLINT",
		schema.TypeInt8:      "TINYINT",
		schema.TypeUint64:    "BIGINT",
		schema.TypeUint32:    "INT",
		schema.TypeUint16:    "SMALLINT",
		schema.TypeUint8:     "TINYINT",
		schema.TypeFloat32:   "FLOAT",
		schema.TypeFloat64:   "FLOAT",
		schema.TypeBytes:     "TEXT",
		schema.TypeString:    "TEXT",
		schema.TypeBoolean:   "BIT",
		schema.TypeAny:       "JSON",
		schema.TypeDate:      "DATE",
		schema.TypeDatetime:  "TIMESTAMP",
		schema.TypeTimestamp: "TIMESTAMP",
	})
}

func ClearOriginalType(colSchema abstract.ColSchema) string {
	clearTyp := strings.ToUpper(strings.TrimPrefix(colSchema.OriginalType, "mysql:"))
	// we do not need it
	if strings.Contains(clearTyp, "ENUM") {
		return "ENUM"
	}
	if strings.Contains(clearTyp, "SET") {
		return "SET"
	}
	clearTyp = strings.ReplaceAll(clearTyp, " ZEROFILL", "")
	clearTyp = regexp.MustCompile(`\(\d*\)`).ReplaceAllString(clearTyp, "")
	clearTyp = regexp.MustCompile(`\(\d*\,\d*\)`).ReplaceAllString(clearTyp, "")
	return clearTyp
}
