//go:build !disable_clickhouse_provider

package clickhouse

import (
	"sort"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/set"
)

type Schema struct {
	cols               []abstract.ColSchema
	systemColumnsFirst bool
	name               string
}

//-------------------------------------------------------------------------------------
// funny dichotomy - we divide all columns into two types here:
// 1) dataKeys()   - which includes PrimaryKey.
//                   dataKeys() returns array of columns, where firstly goes PrimaryKey
// 2) systemKeys()

// WORKAROUND TO BACK COMPATIBILITY WITH 'SYSTEM KEYS' - see TM-5087

var genericParserSystemCols = set.New(
	"_logfeller_timestamp",
	"_timestamp",
	"_partition",
	"_offset",
	"_idx",
)

func isSystemKeysPartOfPrimary(in []abstract.ColSchema) bool {
	count := 0
	for _, el := range in {
		if genericParserSystemCols.Contains(el.ColumnName) && el.PrimaryKey {
			count++
		}
	}
	return count == 4 || count == 5
}

func (s *Schema) dataKeys() []abstract.ColSchema {
	res := make([]abstract.ColSchema, 0)
	for _, col := range s.cols {
		if !genericParserSystemCols.Contains(col.ColumnName) {
			res = append(res, col)
		}
	}

	sort.SliceStable(res, func(i, j int) bool {
		return res[i].PrimaryKey && (!res[j].PrimaryKey)
	})

	return res
}

func (s *Schema) systemKeys() []abstract.ColSchema {
	res := make([]abstract.ColSchema, 0)
	for _, col := range s.cols {
		if genericParserSystemCols.Contains(col.ColumnName) {
			res = append(res, abstract.ColSchema{
				TableSchema:  "",
				TableName:    "",
				Path:         "",
				ColumnName:   col.ColumnName,
				DataType:     col.DataType,
				PrimaryKey:   true,
				FakeKey:      false,
				Required:     true, // system columns required
				Expression:   "",
				OriginalType: "",
				Properties:   nil,
			})
		}
	}

	return res
}

func (s *Schema) abstractCols() []abstract.ColSchema {
	if isSystemKeysPartOfPrimary(s.cols) {
		if s.systemColumnsFirst {
			return append(s.systemKeys(), s.dataKeys()...)
		}
		return append(s.dataKeys(), s.systemKeys()...)
	}
	return s.cols
}

func NewSchema(cols []abstract.ColSchema, systemColumnsFirst bool, name string) *Schema {
	return &Schema{
		cols:               cols,
		systemColumnsFirst: systemColumnsFirst,
		name:               name,
	}
}
