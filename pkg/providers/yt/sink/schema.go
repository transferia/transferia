//go:build !disable_yt_provider

package sink

import (
	"slices"
	"strconv"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/ptr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
)

const shardIndexColumnName = "_shard_key"

type Schema struct {
	path   ypath.Path
	cols   []abstract.ColSchema
	config yt.YtDestinationModel
}

func (s *Schema) PrimaryKeys() []abstract.ColSchema {
	res := make([]abstract.ColSchema, 0)
	for _, col := range s.Cols() {
		if col.PrimaryKey {
			res = append(res, col)
		}
	}

	return res
}

func (s *Schema) DataKeys() []abstract.ColSchema {
	res := make([]abstract.ColSchema, 0)
	for _, col := range s.cols {
		if col.ColumnName == shardIndexColumnName {
			continue
		}
		if !col.PrimaryKey {
			res = append(res, col)
		}
	}
	keys := make([]abstract.ColSchema, 0)
	for _, col := range s.cols {
		if col.PrimaryKey {
			keys = append(keys, col)
		}
	}
	return append(keys, res...)
}

func (s *Schema) BuildSchema(schemas []abstract.ColSchema) (*schema.Schema, error) {
	target := schema.Schema{
		UniqueKeys: true,
		Strict:     ptr.Bool(true),
		Columns:    make([]schema.Column, len(schemas)),
	}
	haveDataColumns := false
	haveKeyColumns := false
	for i, col := range schemas {
		target.Columns[i] = schema.Column{
			Name:       col.ColumnName,
			Type:       fixDatetime(&col),
			Expression: col.Expression,
		}
		if col.PrimaryKey {
			target.Columns[i].SortOrder = schema.SortAscending
			if target.Columns[i].Type == schema.TypeAny {
				target.Columns[i].Type = schema.TypeString // should not use any as keys
			}
			haveKeyColumns = true
		} else {
			haveDataColumns = true
		}
	}
	if !haveKeyColumns {
		return nil, abstract.NewFatalError(NoKeyColumnsFound)
	}
	if !haveDataColumns {
		target.Columns = append(target.Columns, schema.Column{
			Name:     DummyMainTable,
			Type:     "any",
			Required: false,
		})
	}
	return &target, nil
}

func pivotKeys(cols []abstract.ColSchema, config yt.YtDestinationModel) (pivots []interface{}) {
	pivots = []interface{}{make([]interface{}, 0)}
	countK := 0
	for _, col := range cols {
		if col.PrimaryKey {
			countK++
		}
	}

	if countK == 0 {
		return pivots
	}

	if cols[0].ColumnName == shardIndexColumnName {
		for i := 0; i < config.TimeShardCount(); i++ {
			key := make([]interface{}, countK)
			key[0] = uint64(i)
			pivots = append(pivots, key)
		}
	}

	return pivots
}

func (s *Schema) PivotKeys() (pivots []interface{}) {
	return pivotKeys(s.Cols(), s.config)
}

func GetCols(s schema.Schema) []abstract.ColSchema {
	var cols []abstract.ColSchema
	for _, column := range s.Columns {
		var col abstract.ColSchema
		col.ColumnName = column.Name
		if column.SortOrder != schema.SortNone {
			col.PrimaryKey = true
		}
		cols = append(cols, col)
	}
	return cols
}

func BuildDynamicAttrs(cols []abstract.ColSchema, config yt.YtDestinationModel) map[string]interface{} {
	attrs := map[string]interface{}{
		"primary_medium":            config.PrimaryMedium(),
		"optimize_for":              config.OptimizeFor(),
		"tablet_cell_bundle":        config.CellBundle(),
		"chunk_writer":              map[string]interface{}{"prefer_local_host": false},
		"enable_dynamic_store_read": true,
		"atomicity":                 string(config.Atomicity()),
	}

	if config.TTL() > 0 {
		attrs["min_data_versions"] = 0
		attrs["max_data_versions"] = 1
		attrs["merge_rows_on_flush"] = true
		attrs["min_data_ttl"] = 0
		attrs["auto_compaction_period"] = config.TTL()
		attrs["max_data_ttl"] = config.TTL()
	}
	if config.TimeShardCount() > 0 {
		attrs["tablet_balancer_config"] = map[string]interface{}{"enable_auto_reshard": false}
		attrs["pivot_keys"] = pivotKeys(cols, config)
		attrs["backing_store_retention_time"] = 0
	}

	return config.MergeAttributes(attrs)
}

func (s *Schema) Attrs() map[string]interface{} {
	attrs := BuildDynamicAttrs(s.Cols(), s.config)
	attrs["dynamic"] = true
	return attrs
}

func (s *Schema) ShardCol() (abstract.ColSchema, string) {
	var defaultVal abstract.ColSchema

	if s.config.TimeShardCount() <= 0 || s.config.HashColumn() == "" {
		return defaultVal, ""
	}

	hashC := s.config.HashColumn()
	if !slices.ContainsFunc(s.cols, func(c abstract.ColSchema) bool { return c.ColumnName == hashC }) {
		return defaultVal, ""
	}

	shardE := "farm_hash(" + hashC + ") % " + strconv.Itoa(s.config.TimeShardCount())
	colSch := abstract.MakeTypedColSchema(shardIndexColumnName, string(schema.TypeUint64), true)
	colSch.Expression = shardE

	return colSch, hashC
}

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

func dataKeysSystemKeys(in []abstract.ColSchema) ([]abstract.ColSchema, []abstract.ColSchema) { // returns: systemK, dataK
	systemK := make([]abstract.ColSchema, 0, 4)
	dataK := make([]abstract.ColSchema, 0, len(in)-4)

	for _, el := range in {
		if genericParserSystemCols.Contains(el.ColumnName) {
			systemK = append(systemK, el)
		} else {
			dataK = append(dataK, el)
		}
	}
	return systemK, dataK
}

//----------------------------------------------------

func (s *Schema) Cols() []abstract.ColSchema {
	dataK := s.DataKeys()
	col, key := s.ShardCol()
	res := make([]abstract.ColSchema, 0)
	if key != "" {
		res = append(res, col)
	}

	if isSystemKeysPartOfPrimary(dataK) {
		systemK, newDataK := dataKeysSystemKeys(dataK)
		res = append(res, systemK...)
		res = append(res, newDataK...)
	} else {
		res = append(res, dataK...)
	}

	logger.Log.Debug("Compiled cols", log.Any("res", res))
	return res
}

func (s *Schema) Table() (migrate.Table, error) {
	currSchema, err := s.BuildSchema(s.Cols())
	if err != nil {
		return migrate.Table{}, err
	}
	return migrate.Table{
		Attributes: s.Attrs(),
		Schema:     *currSchema,
	}, nil
}

func removeDups(slice []abstract.ColSchema) []abstract.ColSchema {
	unique := map[columnName]struct{}{}
	result := make([]abstract.ColSchema, 0, len(slice))
	for _, item := range slice {
		if _, ok := unique[item.ColumnName]; ok {
			continue
		}
		unique[item.ColumnName] = struct{}{}
		result = append(result, item)
	}
	return result
}

func (s *Schema) IndexTables() map[ypath.Path]migrate.Table {
	res := make(map[ypath.Path]migrate.Table)
	for _, k := range s.config.Index() {
		if k == s.config.HashColumn() {
			continue
		}

		var valCol abstract.ColSchema
		found := false
		for _, col := range s.Cols() {
			if col.ColumnName == k {
				valCol = col
				valCol.PrimaryKey = true
				found = true
				break
			}
		}
		if !found {
			continue
		}

		pKeys := s.PrimaryKeys()
		if len(pKeys) > 0 && pKeys[0].ColumnName == shardIndexColumnName {
			// we should not duplicate sharder
			pKeys = pKeys[1:]
		}
		shardCount := s.config.TimeShardCount()
		var idxCols []abstract.ColSchema
		if shardCount > 0 {
			shardE := "farm_hash(" + k + ") % " + strconv.Itoa(shardCount)
			shardCol := abstract.MakeTypedColSchema(shardIndexColumnName, string(schema.TypeUint64), true)
			shardCol.Expression = shardE

			idxCols = append(idxCols, shardCol)
		}
		idxCols = append(idxCols, valCol)
		idxCols = append(idxCols, pKeys...)
		idxCols = append(idxCols, abstract.MakeTypedColSchema(DummyIndexTable, "any", false))
		idxCols = removeDups(idxCols)
		idxAttrs := s.Attrs()

		idxPath := ypath.Path(MakeIndexTableName(s.path.String(), k))
		schema, err := s.BuildSchema(idxCols)
		if err != nil {
			panic(err)
		}
		res[idxPath] = migrate.Table{
			Attributes: s.config.MergeAttributes(idxAttrs),
			Schema:     *schema,
		}
	}

	return res
}

func tryHackType(col abstract.ColSchema) string { // it works only for legacy things for back compatibility
	if col.PrimaryKey {
		// _timestamp - it's from generic_parser - 'system' column for type-system version <= 4.
		//     For type-system version >4 field _timestamp already has type 'schema.TypeTimestamp'
		// write_time - it's for kafka-without-parser source.
		//     Actually we don't allow to create such transfers anymore - but there are 3 running transfers: dttrnh0ga3aonditp61t,dttvu1t4kbncbmd91s4p,dtts220jnibnl4cm64ar
		if col.ColumnName == "_timestamp" || col.ColumnName == "write_time" {
			switch col.DataType {
			case "DateTime", "datetime":
				return string(schema.TypeInt64)
			}
		}
	}

	return col.DataType
}

func NewSchema(cols []abstract.ColSchema, config yt.YtDestinationModel, path ypath.Path) *Schema {
	columnsWithoutExpression := make([]abstract.ColSchema, len(cols))
	for i := range cols {
		columnsWithoutExpression[i] = cols[i]
		columnsWithoutExpression[i].Expression = ""
	}
	return &Schema{
		path:   path,
		cols:   columnsWithoutExpression,
		config: config,
	}
}
