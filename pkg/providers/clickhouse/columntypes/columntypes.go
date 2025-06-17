//go:build !disable_clickhouse_provider

package columntypes

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	numbersExtractorRe = regexp.MustCompile("[0-9]+")
	intRe              = regexp.MustCompile(`^U?Int(\d{1,3})$`)
)

type TypeDescription struct {
	typ                 string
	dateTime64Precision int

	IsString     bool
	IsDecimal    bool
	IsArray      bool
	IsDateTime64 bool
	IsDateTime   bool
	IsDate       bool
	YtBaseType   string
	IsInteger    bool
}

type TypeMapping map[string]*TypeDescription

func NewTypeDescription(chTypeName string) *TypeDescription {
	baseType := BaseType(chTypeName)
	ytType, _ := ToYtType(baseType)
	var dateTime64Precision int
	if baseType == "DateTime64" {
		// We are looking for second number in string (first always 64)
		parts := numbersExtractorRe.FindAllString(chTypeName, 2)
		if len(parts) > 1 {
			dateTime64Precision, _ = strconv.Atoi(parts[1])
		}
	}
	return &TypeDescription{
		typ:                 chTypeName,
		dateTime64Precision: dateTime64Precision,

		IsDateTime64: baseType == "DateTime64",
		IsDateTime:   baseType == "DateTime",
		IsDate:       baseType == "Date",
		IsString:     baseType == "String",
		IsArray:      strings.Contains(baseType, "Array"),
		IsDecimal:    baseType == "Decimal",
		IsInteger:    intRe.MatchString(baseType),
		YtBaseType:   ytType,
	}
}

func (t *TypeDescription) DateTime64Precision() int {
	return t.dateTime64Precision
}

func LegacyIsDecimal(originalType string) bool {
	// Old behavior for homo transfers
	return strings.HasPrefix(originalType, "ch:Decimal(") ||
		strings.HasPrefix(originalType, "ch:Nullable(Decimal(")
}
