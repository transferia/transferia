package sink

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	ytsdk "go.ytsaurus.tech/yt/go/yt"
)

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
		if col.ColumnName == "_shard_key" {
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
	ss := true
	target := schema.Schema{
		UniqueKeys: true,
		Strict:     &ss,
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
			Name:     "__dummy",
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

	if cols[0].ColumnName == "_shard_key" {
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
	}

	atomicity := string(ytsdk.AtomicityNone)
	if config.Atomicity() != "" {
		atomicity = string(config.Atomicity())
	}
	attrs["atomicity"] = atomicity

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
		if config.AutoFlushPeriod() > 0 {
			attrs["dynamic_store_auto_flush_period"] = config.AutoFlushPeriod()
		}
	}

	if !config.Spec().IsEmpty() {
		for k, v := range config.Spec().GetConfig() {
			attrs[k] = v
		}
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

	if s.config.TimeShardCount() <= 0 {
		return defaultVal, ""
	}

	if s.config.HashColumn() == "" {
		return defaultVal, ""
	}

	var hashC string
	if s.config.HashColumn() != "" {
		hashC = s.config.HashColumn()
	}

	found := false
	for _, c := range s.cols {
		if c.ColumnName == hashC {
			found = true
		}
	}

	for _, c := range s.DataKeys() {
		if c.ColumnName == hashC {
			found = true
		}
	}

	if !found {
		return defaultVal, ""
	}

	shardCount := s.config.TimeShardCount()
	if shardCount == 0 {
		shardCount = 1
	}

	shardE := "farm_hash(" + hashC + ") % " + strconv.Itoa(shardCount)
	colSch := abstract.MakeTypedColSchema("_shard_key", string(schema.TypeUint64), true)
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
	if strings.HasSuffix(s.path.String(), "/_ping") {
		return res
	}
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
		if len(pKeys) > 0 && strings.Contains(pKeys[0].Expression, "farm_hash") {
			// we should not duplicate sharder
			pKeys = pKeys[1:]
		}
		shardCount := s.config.TimeShardCount()
		var idxCols []abstract.ColSchema
		if shardCount > 0 {
			shardE := "farm_hash(" + k + ") % " + strconv.Itoa(shardCount)
			shardCol := abstract.MakeTypedColSchema("_shard_key", string(schema.TypeUint64), true)
			shardCol.Expression = shardE

			idxCols = append(idxCols, shardCol)
		}
		idxCols = append(idxCols, valCol)
		idxCols = append(idxCols, pKeys...)
		idxCols = append(idxCols, abstract.MakeTypedColSchema("_dummy", "any", false))
		idxCols = removeDups(idxCols)
		idxAttrs := map[string]interface{}{
			"dynamic":            true,
			"primary_medium":     s.config.PrimaryMedium(), // "ssd_blobs",
			"atomicity":          "none",
			"optimize_for":       "lookup",
			"tablet_cell_bundle": s.config.CellBundle(),
			"pivot_keys":         s.PivotKeys(),
			"chunk_writer":       map[string]interface{}{"prefer_local_host": false},
		}
		atomicity := string(s.config.Atomicity())
		if atomicity != "" {
			idxAttrs["atomicity"] = atomicity
		}
		if s.config.TTL() > 0 {
			idxAttrs["merge_rows_on_flush"] = true
			idxAttrs["min_data_ttl"] = 0
			idxAttrs["auto_compaction_period"] = s.config.TTL()
			idxAttrs["max_data_ttl"] = s.config.TTL()
		}

		idxPath := ypath.Path(fmt.Sprintf("%v__idx_%v", s.path, k))
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
