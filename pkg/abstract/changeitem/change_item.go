package changeitem

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract/dterrors"
	"github.com/transferria/transferria/pkg/util"
	"github.com/transferria/transferria/pkg/util/jsonx"
	"github.com/transferria/transferria/pkg/util/set"
	"go.ytsaurus.tech/yt/go/yson"
)

var (
	_ json.Marshaler   = (*ChangeItem)(nil)
	_ json.Unmarshaler = (*ChangeItem)(nil)
	_ yson.Marshaler   = (*ChangeItem)(nil)
	_ yson.Unmarshaler = (*ChangeItem)(nil)

	ysonEncoderOptions *yson.EncoderOptions = nil // for tests // nolint:staticcheck
)

type ChangeItem struct {
	ID         uint32 // Transaction ID. For snapshot: id==0, for mongo on replication is also 0.
	LSN        uint64 // it's ok to be 0 for snapshot
	CommitTime uint64
	Counter    int
	// This field isn't used a lot in code
	// but in __WAL txPosition used as part of composite primary key
	// so, it's very important field!
	Kind   Kind   // insert/update/delete/truncate/DDL/pg:DDL
	Schema string // for pg: filled by wal2json - contains "public", for ydb: always empty string
	Table  string // for pg: filled by wal2json - contains tableName, for ydb: depends on 'UseFullPaths'
	// PartID is used to identify part of sharded upload to make it possible to distinguish items which belong to different parts.
	// PartIDs must be unique within a single table. Each source may use its own PartID generation rules.
	// Sinker should interpret PartID as a plain string to be used only to distinguish one part from another and should not attempt to parse or decode it somehow.
	// PartID should contain only characters that may be used in table identifiers (latin characters, digits, '-' and '_' chars)
	PartID string

	// ColumnNames is a write-once field. Its value may be reused across multiple ChangeItems.
	// Do not use this field in new code! Use column name from table schema instead.
	// This field may be empty when the table schema is not.
	ColumnNames  []string
	ColumnValues []interface{}

	// TableSchema is a write-once field. Its value may be reused across multiple ChangeItems
	//	schema filled on all rows event + init / done table load
	TableSchema *TableSchema

	// OldKeys is a set of (PRIMARY) keys identifying the previous version of the tuple which this item represents.
	//
	// There are also special cases when this field contains different data:
	//   - PG source: contains PRIMARY KEY fields as well as other fields for which UNIQUE indexes exist. Note that when there is no PRIMARY KEY, PG source considers one of the UNIQUE indexes to be the PRIMARY KEY. When REPLICA IDENTITY FULL is set, contains all fields.
	//   - MySQL source: contains the same fields as columnNames or keys, depends on binlog_row_image.
	//
	// There may be other specific cases in addition to the ones mentioned above. In any case, one must rely on TableSchema, not on the presence or absence of particular columns inside this field.
	OldKeys OldKeysType

	TxID string // this only valid for mysql so far, for now we will not store it in YT

	Query string    // optional, can be filled only for items from mysql binlogs with binlog_rows_query_log_events enabled
	Size  EventSize // optional event size
}

func (c *ChangeItem) IsTxDone() bool {
	if c.Kind == DDLKind && len(c.ColumnNames) == 1 && len(c.ColumnValues) == 1 && c.ColumnNames[0] == "query" {
		switch t := c.ColumnValues[0].(type) {
		case string:
			return strings.HasPrefix(t, "-- transaction ") && strings.HasSuffix(t, " done")
		}
	}
	return false
}

// EnsureSanity ensures the given item is a sound and sane one which follows the contract between item's fields.
//
// It should be removed one day when the contract is ingrained in the item's structure.
func (c *ChangeItem) EnsureSanity() error {
	if len(c.ColumnNames) != len(c.ColumnValues) {
		return dterrors.NewFatalError(xerrors.Errorf("len(ColumnNames)=%d <> len(ColumnValues)=%d", len(c.ColumnNames), len(c.ColumnValues)))
	}
	return nil
}

const (
	offsetCol    = "_offset"
	partitionCol = "_partition"
)

var RowEventKinds = set.New(InsertKind, UpdateKind, DeleteKind, MongoUpdateDocumentKind)
var SystemKinds = set.New(
	InitTableLoad, InitShardedTableLoad, DoneTableLoad, DoneShardedTableLoad, DropTableKind, TruncateTableKind,
	SynchronizeKind,
)

func (c *ChangeItem) Offset() (offset uint64, found bool) {
	if len(c.ColumnNames) < 3 {
		found = false
		return offset, found
	}
	for i, n := range c.ColumnNames {
		if n == offsetCol {
			if v, ok := c.ColumnValues[i].(uint64); ok {
				return v, true
			}
		}
	}
	return offset, found
}

func (c *ChangeItem) Part() string {
	for i, n := range c.ColumnNames {
		if n == partitionCol {
			if v, ok := c.ColumnValues[i].(string); ok {
				return v
			}
		}
	}
	return "default"
}

func (c *ChangeItem) Fqtn() string {
	return c.Schema + "_" + c.Table
}

func (c *ChangeItem) TableID() TableID {
	return TableID{Namespace: c.Schema, Name: c.Table}
}

func (c *ChangeItem) TablePartID() TablePartID {
	return TablePartID{TableID: c.TableID(), PartID: c.PartID}
}

func (c *ChangeItem) PgName() string {
	return PgName(c.Schema, c.Table)
}

func PgName(schemaName, tableName string) string {
	return NewTableID(schemaName, tableName).Fqtn()
}

func (c *ChangeItem) AsMap() map[string]interface{} {
	res := make(map[string]interface{}, len(c.ColumnNames))
	for i := range c.ColumnNames {
		res[c.ColumnNames[i]] = c.ColumnValues[i]
	}
	return res
}

func (c *ChangeItem) ColumnNameIndex(columnName string) int {
	for i, currColumnName := range c.ColumnNames {
		if currColumnName == columnName {
			return i
		}
	}
	return -1
}

func (c *ChangeItem) ColumnNameIndices() map[string]int {
	result := make(map[string]int, len(c.ColumnNames))
	for i, columnName := range c.ColumnNames {
		result[columnName] = i
	}
	return result
}

func (c *ChangeItem) KeyVals() []string {
	fastTableSchema := MakeFastTableSchema(c.TableSchema.Columns())
	res := make([]string, 0)
	for i, name := range c.ColumnNames {
		if col, ok := fastTableSchema[ColumnName(name)]; ok && col.IsKey() {
			res = append(res, fmt.Sprintf("%v", c.ColumnValues[i]))
		}
	}
	return res
}

func (c *ChangeItem) KeysAsMap() map[string]interface{} {
	fastTableSchema := MakeFastTableSchema(c.TableSchema.Columns())
	res := make(map[string]interface{})
	for i, name := range c.ColumnNames {
		if col, ok := fastTableSchema[ColumnName(name)]; ok && col.IsKey() {
			res[c.ColumnNames[i]] = c.ColumnValues[i]
		}
	}
	return res
}

func (c *ChangeItem) KeyCols() []string {
	return KeyNames(c.TableSchema.Columns())
}

func (c *ChangeItem) MakeMapKeys() map[string]bool {
	keyCols := map[string]bool{}
	for _, col := range c.TableSchema.Columns() {
		if col.IsKey() {
			keyCols[col.ColumnName] = true
		}
	}
	return keyCols
}

// ColumnName is an explicitly declared column name
type ColumnName string

// FastTableSchema is a mapping from column name to its schema
type FastTableSchema map[ColumnName]ColSchema

// MakeFastTableSchema produces a fast table schema from an array of schemas of each column. Column names are taken from these schemas.
func MakeFastTableSchema(slowTableSchema []ColSchema) FastTableSchema {
	result := make(FastTableSchema)
	for i, colSchema := range slowTableSchema {
		result[ColumnName(colSchema.ColumnName)] = slowTableSchema[i]
	}
	return result
}

// KeysChanged - checks if update changes primary keys
// NOTE: this method is quite inefficient
func (c *ChangeItem) KeysChanged() bool {
	if c.Kind != UpdateKind {
		return false
	}

	// Change item validity checks
	if len(c.OldKeys.KeyNames) != len(c.OldKeys.KeyValues) {
		return false
	}
	if len(c.ColumnNames) != len(c.ColumnValues) {
		return false
	}

	// We could drop this loop at all and iterate over c.OldKeys to traverse all the primary key columns,
	// but the problem is that MySQL source puts all the columns into c.OldKeys for update items instead
	// of putting only the primary key ones. This is clearly a bug and it should be fixed, but MySQL sink
	// appears to actually rely on this bug and will break once it is fixed. For now we will just stick
	// with looping over TableSchema.
	for i := range c.TableSchema.Columns() {
		pkeyCol := c.TableSchema.Columns()[i]
		if !pkeyCol.PrimaryKey {
			continue
		}

		var oldKeyValue, newKeyValue interface{}

		// All key columns usually appear at the beginning of the table schema, so using linear search is
		// probably sufficient as we will break the loop early
		for i, name := range c.OldKeys.KeyNames {
			if name != pkeyCol.ColumnName {
				continue
			}
			oldKeyValue = c.OldKeys.KeyValues[i]
			break
		}
		for i, name := range c.ColumnNames {
			if name != pkeyCol.ColumnName {
				continue
			}
			newKeyValue = c.ColumnValues[i]
			break
		}

		// TODO: we should use some interface with Equals() method instead of empty interface to avoid calling reflect.DeepEqual
		if !reflect.DeepEqual(oldKeyValue, newKeyValue) {
			return true
		}
	}
	return false
}

func (c *ChangeItem) IsRowEvent() bool {
	return RowEventKinds.Contains(c.Kind)
}

func (c *ChangeItem) IsSystemKind() bool {
	return SystemKinds.Contains(c.Kind) || c.IsSystemTable()
}

// IsToasted check is change item contains all old-values for update/delete kinds.
func (c *ChangeItem) IsToasted() bool {
	if !c.IsRowEvent() {
		return false
	}
	if c.Kind == InsertKind {
		return false
	}
	if c.Kind == UpdateKind {
		return len(c.ColumnNames) != len(c.TableSchema.Columns())
	}
	if c.Kind == DeleteKind {
		return len(c.OldKeys.KeyNames) != len(c.TableSchema.Columns())
	}
	return false
}

// OldOrCurrentKeys returns a string representing the values of the columns from the given set of columns, extracted from OldKeys or ColumnValues, if OldKeys are absent
func (c *ChangeItem) OldOrCurrentKeysString(keyColumns map[string]bool) string {
	if (c.Kind == UpdateKind || c.Kind == DeleteKind) && len(c.OldKeys.KeyValues) > 0 {
		keys := make(map[string]interface{})
		for k := range keyColumns {
			keys[k] = nil
		}

		for i, keyName := range c.OldKeys.KeyNames {
			if keyColumns[keyName] {
				keys[keyName] = c.OldKeys.KeyValues[i]
			}
		}

		toMarshal := make([]interface{}, len(keys))
		for i, colName := range util.MapKeysInOrder(keys) {
			toMarshal[i] = keys[colName]
		}

		d, _ := json.Marshal(toMarshal)
		return string(d)
	}

	return c.CurrentKeysString(keyColumns)
}

func (c *ChangeItem) CurrentKeysString(keyColumns map[string]bool) string {
	keys := make(map[string]interface{})
	for k := range keyColumns {
		keys[k] = nil
	}

	for i, colName := range c.ColumnNames {
		if keyColumns[colName] {
			keys[colName] = c.ColumnValues[i]
		}
	}

	toMarshal := make([]interface{}, len(keys))
	for i, colName := range util.MapKeysInOrder(keys) {
		toMarshal[i] = keys[colName]
	}

	d, _ := json.Marshal(toMarshal)
	return string(d)
}

func ContainsNonRowItem(s []ChangeItem) bool {
	for i := range s {
		if !s[i].IsRowEvent() {
			return true
		}
	}
	return false
}

// FindItemOfKind returns an item in the slice whose kind is among the given ones, or nil if there is no such item in it
func FindItemOfKind(s []ChangeItem, kinds ...Kind) *ChangeItem {
	wanted := make(map[Kind]bool)
	for _, k := range kinds {
		wanted[k] = true
	}
	for i := range s {
		if wanted[s[i].Kind] {
			return &s[i]
		}
	}
	return nil
}

// IsMirror
// mirror - it's special format with hardcoded schema - used for "mirroring" (queue->queue) - transfer messages between queues without changes
func (c *ChangeItem) IsMirror() bool {
	if len(c.ColumnNames) != len(RawDataColumns) {
		return false
	}
	for i := range c.ColumnNames {
		if c.ColumnNames[i] != RawDataColumns[i] {
			return false
		}
	}
	return true
}

func (c *ChangeItem) IsSystemTable() bool {
	return IsSystemTable(c.Table)
}

func (c *ChangeItem) SetTableID(tableID TableID) {
	c.Schema = tableID.Namespace
	c.Table = tableID.Name
}

func ChangeItemFromMap(input map[string]interface{}, schema *TableSchema, table string, kind string) ChangeItem {
	cols := make([]string, len(schema.Columns()))
	vals := make([]interface{}, len(schema.Columns()))
	for i, k := range schema.Columns() {
		cols[i] = k.ColumnName
		vals[i] = input[k.ColumnName]
	}

	return ChangeItem{
		ID:         0,
		LSN:        0,
		CommitTime: 0,
		Counter:    0,
		Schema:     "",
		OldKeys: OldKeysType{
			KeyNames:  nil,
			KeyTypes:  nil,
			KeyValues: nil,
		},
		TxID:         "",
		Kind:         Kind(kind),
		Table:        table,
		PartID:       "",
		ColumnNames:  cols,
		ColumnValues: vals,
		TableSchema:  schema,
		Query:        "",
		Size:         EmptyEventSize(),
	}
}

type PropertyKey string

func (c *ChangeItem) ToJSONString() string {
	t, _ := json.Marshal(*c)
	return string(t)
}

func (c ChangeItem) MarshalYSON() ([]byte, error) {
	var buf bytes.Buffer
	writer := yson.NewWriter(&buf)

	writer.BeginMap()

	writer.MapKeyString("id")
	writer.Uint64(uint64(c.ID))
	writer.MapKeyString("nextlsn")
	writer.Uint64(uint64(c.LSN))
	writer.MapKeyString("commitTime")
	writer.Uint64(uint64(c.CommitTime))
	writer.MapKeyString("txPosition")
	writer.Int64(int64(c.Counter))
	writer.MapKeyString("kind")
	writer.String(string(c.Kind))
	writer.MapKeyString("schema")
	writer.String(c.Schema)
	writer.MapKeyString("table")
	writer.String(c.Table)
	writer.MapKeyString("columnnames")
	writer.Any(c.ColumnNames)
	writer.MapKeyString("columnvalues")
	writer.Any(c.ColumnValues)
	writer.MapKeyString("table_schema")
	if c.TableSchema != nil {
		writer.Any(c.TableSchema.columns)
	} else {
		writer.Any([]ColSchema(nil))
	}
	writer.MapKeyString("oldkeys")
	writer.Any(c.OldKeys)
	writer.MapKeyString("tx_id")
	writer.String(c.TxID)

	writer.EndMap()

	if err := writer.Finish(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *ChangeItem) UnmarshalYSON(data []byte) error {
	buf := bytes.NewBuffer(data)
	reader := yson.NewReader(buf)

	event, err := reader.Next(true)
	if err != nil {
		return xerrors.Errorf("cannot read next YSON token: %w", err)
	}
	if event != yson.EventBeginMap {
		return xerrors.New("expected map")
	}

	for {
		event, err = reader.Next(true)
		if err != nil {
			return xerrors.Errorf("cannot read next YSON value: %w", err)
		}
		if event == yson.EventEndMap {
			return nil
		}
		if event != yson.EventKey {
			return xerrors.New("expected map key")
		}
		if reader.Type() != yson.TypeString {
			return xerrors.New("expected string")
		}
		key := reader.String()

		rawVal, err := reader.NextRawValue()
		if err != nil {
			return xerrors.Errorf("cannot parse map value for key %s: %w", key, err)
		}
		var valPtr interface{}
		switch key {
		case "id":
			valPtr = &c.ID
		case "nextlsn":
			valPtr = &c.LSN
		case "commitTime":
			valPtr = &c.CommitTime
		case "txPosition":
			valPtr = &c.Counter
		case "kind":
			valPtr = &c.Kind
		case "schema":
			valPtr = &c.Schema
		case "table":
			valPtr = &c.Table
		case "columnnames":
			valPtr = &c.ColumnNames
		case "columnvalues":
			valPtr = &c.ColumnValues
		case "table_schema":
			c.TableSchema = new(TableSchema)
			valPtr = &c.TableSchema.columns
		case "oldkeys":
			valPtr = &c.OldKeys
		case "tx_id":
			valPtr = &c.TxID
		default:
			continue
		}
		if err := yson.Unmarshal(rawVal, valPtr); err != nil {
			return xerrors.Errorf("cannot unmarshal YSON value for key %s: %w", key, err)
		}
	}
}

func (c ChangeItem) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)

	var err error
	var comma bool
	putItem := func(key string, value any) {
		if comma {
			buf.WriteRune(',')
		}
		if err != nil {
			return
		}
		if err = encoder.Encode(key); err != nil {
			return
		}
		buf.WriteRune(':')
		if err = encoder.Encode(value); err != nil {
			return
		}
		comma = true
	}

	buf.WriteRune('{')
	putItem("id", c.ID)
	putItem("nextlsn", c.LSN)
	putItem("commitTime", c.CommitTime)
	putItem("txPosition", c.Counter)
	putItem("kind", c.Kind)
	putItem("schema", c.Schema)
	putItem("table", c.Table)
	putItem("part", c.PartID)
	putItem("columnnames", c.ColumnNames)
	if len(c.ColumnValues) > 0 {
		putItem("columnvalues", c.ColumnValues)
	}
	if c.TableSchema != nil && len(c.TableSchema.columns) > 0 {
		putItem("table_schema", c.TableSchema.columns)
	}
	putItem("oldkeys", c.OldKeys)
	putItem("tx_id", c.TxID)
	putItem("query", c.Query)
	buf.WriteRune('}')

	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *ChangeItem) UnmarshalJSON(jsonData []byte) error {
	buf := bytes.NewBuffer(jsonData)
	decoder := jsonx.NewDefaultDecoder(buf)

	token, err := decoder.Token()
	if err != nil {
		return xerrors.Errorf("expected '{', got %q", token)
	}
	for {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if token == json.Delim('}') {
			return nil
		}
		mapKey, ok := token.(string)
		if !ok {
			return xerrors.Errorf("expected map key, got %q", token)
		}

		switch mapKey {
		case "id":
			err = decoder.Decode(&c.ID)
		case "nextlsn":
			err = decoder.Decode(&c.LSN)
		case "commitTime":
			err = decoder.Decode(&c.CommitTime)
		case "txPosition":
			err = decoder.Decode(&c.Counter)
		case "kind":
			err = decoder.Decode(&c.Kind)
		case "schema":
			err = decoder.Decode(&c.Schema)
		case "table":
			err = decoder.Decode(&c.Table)
		case "part":
			err = decoder.Decode(&c.PartID)
		case "columnnames":
			err = decoder.Decode(&c.ColumnNames)
		case "columnvalues":
			err = decoder.Decode(&c.ColumnValues)
		case "table_schema":
			c.TableSchema = new(TableSchema)
			err = decoder.Decode(&c.TableSchema.columns)
		case "oldkeys":
			err = decoder.Decode(&c.OldKeys)
		case "tx_id":
			err = decoder.Decode(&c.TxID)
		case "query":
			err = decoder.Decode(&c.Query)
		default:
			// unknown field
			var dummy any
			err = decoder.Decode(&dummy)
		}
		if err != nil {
			return xerrors.Errorf("%s: %w", mapKey, err)
		}
	}
}

func (c *ChangeItem) AddTableColumn(column ColSchema) {
	if c.TableSchema == nil {
		c.TableSchema = NewTableSchema(nil)
	}
	c.TableSchema.columns = append(c.TableSchema.columns, column)
}

func (c *ChangeItem) SetTableSchema(tableSchema *TableSchema) {
	c.TableSchema = tableSchema
}

// RemoveColumns mutate change item to skip some columns from it.
// it remove it from column names and column values
func (c *ChangeItem) RemoveColumns(cols ...string) {
	toDelete := map[string]struct{}{}
	for _, col := range cols {
		toDelete[col] = struct{}{}
	}

	colMask := make([]byte, len(c.ColumnNames))
	for i, col := range c.ColumnNames {
		if _, ok := toDelete[col]; !ok {
			colMask[i] = 1
		}
	}
	c.ColumnNames = filterMask(c.ColumnNames, colMask)
	c.ColumnValues = filterMask(c.ColumnValues, colMask)
	if len(c.OldKeys.KeyNames) == 0 {
		return
	}

	oldKeyMask := make([]byte, len(c.OldKeys.KeyNames))
	for i, col := range c.OldKeys.KeyNames {
		if _, ok := toDelete[col]; !ok {
			oldKeyMask[i] = 1
		}
	}

	c.OldKeys.KeyNames = filterMask(c.OldKeys.KeyNames, oldKeyMask)
	c.OldKeys.KeyValues = filterMask(c.OldKeys.KeyValues, oldKeyMask)
	if len(c.OldKeys.KeyTypes) == len(oldKeyMask) {
		c.OldKeys.KeyTypes = filterMask(c.OldKeys.KeyTypes, oldKeyMask)
	}
}

// filterMask remove elements from array based on bit-mask
//
//	items array will be trimmed (edited in-place), so no extra allocations
//	mask is a byte slices same length as items, 1 for keep element and 0 for delete element
//	order of element slices stable, but not preserved.
//	basic idea from leetcode task here: https://www.youtube.com/watch?v=Pcd1ii9P9ZI
func filterMask[T any](items []T, mask []byte) []T {
	k := 0
	for i := range items {
		if mask[i] == 1 {
			items[k] = items[i]
			k++
		}
	}
	return items[:k]
}
