package mask

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	tostring "github.com/transferia/transferia/pkg/transformer/registry/to_string"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const MaskFieldTransformerType = abstract.TransformerType("mask_field")

type HmacHasher struct {
	Tables      filter.Filter
	Columns     *set.Set[string]
	HashFactory func() hash.Hash
	Salt        string
	lgr         log.Logger
}

func (hh *HmacHasher) hash(columnValue any, columnType string) string {
	sig := hmac.New(hh.HashFactory, []byte(hh.Salt))
	sig.Write([]byte(tostring.SerializeToString(columnValue, columnType)))
	return hex.EncodeToString(sig.Sum(nil))
}

func (hh *HmacHasher) schema(original *abstract.TableSchema) *abstract.TableSchema {
	result := make([]abstract.ColSchema, 0, len(original.Columns()))
	for _, col := range original.Columns() {
		copyCol := col
		if hh.Columns.Contains(copyCol.ColumnName) {
			copyCol.DataType = ytschema.TypeString.String()
			copyCol.OriginalType = ""
		}
		result = append(result, copyCol)
	}
	return abstract.NewTableSchema(result)
}

func (hh *HmacHasher) Type() abstract.TransformerType {
	return MaskFieldTransformerType
}

func (hh *HmacHasher) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)

	for _, ci := range input {
		fastCols := ci.TableSchema.FastColumns()
		for i, name := range ci.ColumnNames {
			if hh.Columns.Contains(name) {
				ci.ColumnValues[i] = hh.hash(
					ci.ColumnValues[i],
					fastCols[abstract.ColumnName(name)].DataType,
				)
			}
		}
		ci.SetTableSchema(hh.schema(ci.TableSchema))
		transformed = append(transformed, ci)
	}

	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      errors,
	}
}

func (hh *HmacHasher) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	if !filter.MatchAnyTableNameVariant(hh.Tables, table) {
		return false
	}
	if hh.Columns.Empty() {
		return true
	}
	for _, colSchema := range schema.Columns() {
		if hh.Columns.Contains(colSchema.ColumnName) {
			return true
		}
	}
	return false
}

func (hh *HmacHasher) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return hh.schema(original), nil
}

func (hh *HmacHasher) Description() string {
	tablesIncluded := strings.Join(hh.Tables.IncludeRegexp, ",")
	tablesExcluded := strings.Join(hh.Tables.ExcludeRegexp, ",")
	columnsIncluded := strings.Join(hh.Columns.Slice(), ",")

	return fmt.Sprintf("Hash table columns: columns: %s, "+
		"includedtables: %s, excluded tables: %s", columnsIncluded,
		tablesIncluded, tablesExcluded)
}

func NewHmacHasherTransformer(config MaskFunctionHash, lgr log.Logger, tables filter.Filter, columns []string) (abstract.Transformer, error) {
	columnsSet := set.New(columns...)
	return &HmacHasher{
		Tables:      tables,
		Columns:     columnsSet,
		HashFactory: sha256.New,
		Salt:        config.UserDefinedSalt,
		lgr:         lgr,
	}, nil
}
