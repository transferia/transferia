//go:build !disable_clickhouse_provider

package chmapper

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema"
	"github.com/transferia/transferia/pkg/transformer"
	tostring "github.com/transferia/transferia/pkg/transformer/registry/to_string"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const TransformerType = abstract.TransformerType("ch_mapper")

func init() {
	transformer.Register[Config](
		TransformerType,
		func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return New(cfg), nil
		},
	)
}

type ColumnRemap struct {
	Name       string
	Path       string
	Key        bool
	Masked     bool
	Excluded   bool
	Type       string
	TargetType string
	Expression string
}

type TableRemap struct {
	OriginalTable abstract.TableID
	RenamedTable  abstract.TableID
	Columns       []ColumnRemap
	DDL           *DDLParams
}

type DDLParams struct {
	PartitionBy, OrderBy, Engine string
	TTL                          string
	Distributed                  bool
}

type Config struct {
	Table TableRemap
	Salt  string
}

type mapper struct {
	table TableRemap
	salt  string
}

func (r mapper) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	if len(input) == 0 {
		return abstract.TransformerResult{
			Transformed: nil,
			Errors:      nil,
		}
	}
	masterCI := input[0]
	resSchema, err := r.ResultSchema(masterCI.TableSchema)
	if err != nil {
		return abstract.TransformerResult{
			Transformed: nil,
			Errors:      allWithError(input, err),
		}
	}
	transformed := make([]abstract.ChangeItem, 0, len(input))
	for _, item := range input {
		if item.Kind == changeitem.InitShardedTableLoad && r.table.DDL != nil {
			transformed = append(transformed, r.generateDDL(resSchema))
		}
		item.TableSchema = resSchema
		if r.table.RenamedTable != item.TableID() {
			item.Table = r.table.RenamedTable.Name
			item.Schema = r.table.RenamedTable.Namespace
		}
		var colNames []string
		var vals []any
		itemM := item.AsMap()
		for _, col := range r.table.Columns {
			if col.Excluded {
				continue
			}
			var val any
			if col.Path != "" {
				val = itemM[col.Path]
			} else if v, ok := itemM[col.Name]; ok {
				val = v
			} else {
				continue
			}
			colNames = append(colNames, col.Name)
			if col.Masked {
				val = r.hash(val, col.Type)
			}
			vals = append(vals, val)
		}
		item.ColumnValues = vals
		item.ColumnNames = colNames
		transformed = append(transformed, item)
	}
	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      nil,
	}
}

func (r mapper) generateDDL(result *abstract.TableSchema) abstract.ChangeItem {
	bb := strings.Builder{}
	_, _ = bb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` ", r.table.RenamedTable.Name))
	if r.table.DDL.Distributed {
		_, _ = bb.WriteString("ON CLUSTER '{cluster}'\n")
	}
	var cols []string
	cols, keyIsNullable := clickhouse.ColumnDefinitions(result.Columns())
	_, _ = bb.WriteString(fmt.Sprintf(" (\n	%s\n)", strings.Join(cols, ",\n	")))

	bb.WriteString(fmt.Sprintf("\n ENGINE=%s", r.table.DDL.Engine))
	bb.WriteString(fmt.Sprintf("\n ORDER BY (%s)", r.table.DDL.OrderBy))

	if r.table.DDL.PartitionBy != "" {
		bb.WriteString(fmt.Sprintf("\n PARTITION BY (%s)", r.table.DDL.PartitionBy))
	}
	if r.table.DDL.TTL != "" {
		bb.WriteString(fmt.Sprintf("\n TTL %s", r.table.DDL.TTL))
	}
	if keyIsNullable {
		bb.WriteString("\n SETTINGS allow_nullable_key = 1")
	}
	return schema.NewTableDDL(
		r.table.RenamedTable,
		bb.String(),
		r.table.DDL.Engine,
	).ToChangeItem()
}

func (r mapper) hash(columnValue any, columnType string) string {
	sig := hmac.New(sha256.New, []byte(r.salt))
	sig.Write([]byte(tostring.SerializeToString(columnValue, columnType)))
	return hex.EncodeToString(sig.Sum(nil))
}

func (r mapper) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return r.table.OriginalTable == table
}

func (r mapper) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	var cols []changeitem.ColSchema
	for _, remapped := range r.table.Columns {
		if remapped.Excluded {
			continue
		}
		col := changeitem.ColSchema{
			TableSchema:  r.table.RenamedTable.Namespace,
			TableName:    r.table.RenamedTable.Name,
			Path:         remapped.Path,
			ColumnName:   remapped.Name,
			DataType:     original.FastColumns()[changeitem.ColumnName(remapped.Name)].DataType,
			PrimaryKey:   remapped.Key,
			FakeKey:      false,
			Required:     false,
			Expression:   remapped.Expression,
			OriginalType: "ch:" + remapped.TargetType,
			Properties:   nil,
		}
		if remapped.TargetType == "" {
			col.OriginalType = "ch:" + columntypes.ToChType(col.DataType)
		}
		if remapped.Path != "" && remapped.Type != "" {
			col.DataType = remapped.Type
		}
		if remapped.Masked {
			col.DataType = ytschema.TypeString.String()
		}
		cols = append(cols, col)
	}
	res := changeitem.NewTableSchema(cols)
	return res, nil
}

func (r mapper) Description() string {
	return fmt.Sprintf(`re-mapper transformer:
Table: %s -> %s
Columns:
%s

Extra Config:
%v
`, r.table.OriginalTable, r.table.RenamedTable, r.columnsDescription(), r.table.DDL)
}

func (r mapper) Type() abstract.TransformerType {
	return TransformerType
}

func allWithError(input []abstract.ChangeItem, err error) []abstract.TransformerError {
	var res []abstract.TransformerError
	for _, row := range input {
		res = append(res, abstract.TransformerError{
			Input: row,
			Error: err,
		})
	}
	return res
}

func (r mapper) columnsDescription() string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeaderLine(true)
	table.SetRowLine(true)
	table.SetHeader([]string{"Column", "Type", "Key", "Path", "Masked", "Excluded"})
	for _, col := range r.table.Columns {
		table.Append([]string{
			col.Name,
			col.TargetType,
			cast.ToString(col.Key),
			col.Path,
			cast.ToString(col.Masked),
			cast.ToString(col.Excluded),
		})
	}
	table.Render()
	return buf.String()
}

func New(cfg Config) abstract.Transformer {
	return &mapper{
		table: cfg.Table,
		salt:  cfg.Salt,
	}
}
