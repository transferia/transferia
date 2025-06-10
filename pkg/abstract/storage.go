package abstract

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/set"
)

type LoadProgress func(current, progress, total uint64)

type TableDescription struct {
	Schema string // for example - for mysql here are database name
	Name   string
	Filter WhereStatement
	EtaRow uint64 // estimated number of rows in the table
	Offset uint64 // offset (in rows) along the ordering key (not necessary primary key)
}

var NonExistentTableID TableID = *NewTableID("", "")

func BuildIncludeMap(objects []string) (map[TableID]bool, error) {
	includeObjects := map[TableID]bool{}
	var errs util.Errors
	for _, obj := range objects {
		tid, err := ParseTableID(obj)
		if err != nil {
			errs = append(errs, xerrors.Errorf("object: %s: %w", obj, err))
			continue
		}
		includeObjects[*tid] = true
	}
	if len(errs) > 0 {
		return nil, xerrors.Errorf("unable to parse objects: %w", errs)
	}
	return includeObjects, nil
}

func SchemaFilterByObjects(schema DBSchema, objects []string) (DBSchema, error) {
	if objects == nil {
		return schema, nil
	}
	res := make(DBSchema)
	for _, obj := range objects {
		id, err := ParseTableID(obj)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse: %s: %w", obj, err)
		}
		info, ok := schema[*id]
		if !ok {
			return nil, xerrors.Errorf("object %s not found in schema", obj)
		}
		res[*id] = info
	}
	return res, nil
}

func ParseTableID(object string) (*TableID, error) {
	return NewTableIDFromStringPg(object, false)
}

func ParseTableIDs(objects ...string) ([]TableID, error) {
	var res []TableID
	for _, obj := range objects {
		tid, err := ParseTableID(obj)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse table ID: %s: %w", obj, err)
		}
		res = append(res, *tid)
	}
	return res, nil
}

// NewTableIDFromStringPg parses the given FQTN in PostgreSQL syntax to construct a TableID.
func NewTableIDFromStringPg(fqtn string, replaceOmittedSchemaWithPublic bool) (*TableID, error) {
	parts, err := identifierToParts(fqtn)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse identifier '%s' into parts: %w", fqtn, err)
	}

	switch len(parts) {
	case 0:
		return nil, xerrors.Errorf("zero-length identifier")
	case 1:
		if replaceOmittedSchemaWithPublic {
			return &TableID{Namespace: "public", Name: parts[0]}, nil
		}
		return &TableID{Namespace: "", Name: parts[0]}, nil
	case 2:
		return &TableID{Namespace: parts[0], Name: parts[1]}, nil
	default:
		return nil, xerrors.Errorf("identifier '%s' contains %d parts instead of maximum two", fqtn, len(parts))
	}
}

// identifierToParts separates the given identifier into parts separated by dot.
// Each part may be double-quoted and may contain double-escaped double quotes. These are properly parsed; external double quotes are removed.
func identifierToParts(identifier string) ([]string, error) {
	result := make([]string, 0, 2)
	idPartBuilder := strings.Builder{}
	identifierInProgress := false
	insideDoubleQuotedIdentifier := false
	previousWasDoubleQuote := false

overFQTNRunes:
	for i, r := range identifier {
		switch r {
		case '"':
			if !insideDoubleQuotedIdentifier {
				if !identifierInProgress {
					insideDoubleQuotedIdentifier = true
					continue overFQTNRunes
				} else {
					return nil, xerrors.Errorf("\" outside of a double-quoted name part in position [%d]", i-1)
				}
			}
			if !previousWasDoubleQuote {
				previousWasDoubleQuote = true
				continue overFQTNRunes
			}
			previousWasDoubleQuote = false
		case '.':
			if insideDoubleQuotedIdentifier {
				if !previousWasDoubleQuote {
					// just a dot inside an escaped identifier
					break
				}
				// previous character (double quote) was an end of an escaped identifier
				previousWasDoubleQuote = false
				insideDoubleQuotedIdentifier = false
			}
			if !identifierInProgress {
				return nil, xerrors.Errorf("zero-length name part ending in position [%d]", i)
			}
			result = append(result, idPartBuilder.String())
			idPartBuilder.Reset()
			identifierInProgress = false
			continue overFQTNRunes
		default:
			if previousWasDoubleQuote {
				return nil, xerrors.Errorf("unescaped \" in position [%d]", i-1)
			}
		}
		identifierInProgress = true
		_, _ = idPartBuilder.WriteRune(r)
	}

	if !identifierInProgress {
		return nil, xerrors.Errorf("zero-length name part ending in position [%d]", len(identifier)-1)
	}
	if insideDoubleQuotedIdentifier {
		if !previousWasDoubleQuote {
			return nil, xerrors.Errorf("missing ending \" in position [%d]", len(identifier)-1)
		}
		previousWasDoubleQuote = false
		insideDoubleQuotedIdentifier = false
	}
	result = append(result, idPartBuilder.String())
	idPartBuilder.Reset()
	identifierInProgress = false

	return result, nil
}

type TableInfo struct {
	EtaRow uint64
	IsView bool
	Schema *TableSchema
}

type TableMap map[TableID]TableInfo

func (m *TableMap) Copy() TableMap {
	cp := TableMap{}
	for id, info := range *m {
		cp[id] = info
	}
	return cp
}

func (m *TableMap) String(withSchema bool) string {
	mapCopy := make(map[string]TableInfo)
	for k, v := range *m {
		var newTableInfo TableInfo
		newTableInfo.EtaRow = v.EtaRow
		newTableInfo.IsView = v.IsView
		if withSchema {
			newTableInfo.Schema = v.Schema
		}
		mapCopy[k.Fqtn()] = newTableInfo
	}
	result, _ := json.Marshal(mapCopy)
	return string(result)
}

func (m *TableMap) NoKeysTables() []TableID {
	var noKeyTables []TableID
	for tID, tInfo := range *m {
		if !tInfo.Schema.Columns().HasPrimaryKey() && !tInfo.Schema.Columns().HasFakeKeys() && !tInfo.IsView {
			noKeyTables = append(noKeyTables, tID)
		}
	}
	return noKeyTables
}

func (m *TableMap) FakePkeyTables() []TableID {
	var fakePkeyTables []TableID
	for tID, tInfo := range *m {
		if tInfo.Schema.Columns().HasFakeKeys() {
			fakePkeyTables = append(fakePkeyTables, tID)
		}
	}
	return fakePkeyTables
}

func (m *TableMap) ConvertToTableDescriptions() []TableDescription {
	tableDescriptions := make([]TableDescription, 0, len(*m))
	for tID, tInfo := range *m {
		tableDescriptions = append(tableDescriptions, TableDescription{
			Name:   tID.Name,
			Schema: tID.Namespace,
			EtaRow: tInfo.EtaRow,
			Filter: "",
			Offset: 0,
		})
	}
	return tableDescriptions
}

func (m *TableMap) ToDBSchema() DBSchema {
	resultSchema := make(DBSchema)
	for tableID, tableInfo := range *m {
		resultSchema[tableID] = tableInfo.Schema
	}
	return resultSchema
}

func (t *TableDescription) ID() TableID {
	return TableID{Namespace: t.Schema, Name: t.Name}
}

func (t *TableDescription) PartID() string {
	if t.Offset == 0 && t.Filter == "" {
		// This needed for s3, see: https://github.com/transferia/transferia/review/3538625
		return ""
	}

	asJSONString := fmt.Sprintf(`{"schema": "%s","name": "%s","filter": "%s","offset": %d}`,
		t.Schema, t.Name, t.Filter, t.Offset)

	return util.Hash(asJSONString)
}

func (t *TableDescription) Same(table string) bool {
	if t.Name == table {
		return true
	}
	if fmt.Sprintf("%v.%v", t.Schema, t.Name) == table {
		return true
	}
	if fmt.Sprintf("%v.\"%v\"", t.Schema, t.Name) == table {
		return true
	}
	if t.Fqtn() == table {
		return true
	}
	return false
}

func (t *TableDescription) Fqtn() string {
	return t.ID().Fqtn()
}

func (t *TableDescription) String() string {
	return fmt.Sprintf("%s [filter %q offset %d]", t.Fqtn(), t.Filter, t.Offset)
}

// TableIDsIntersection returns an intersection of two lists of TableIDs
func TableIDsIntersection(a []TableID, b []TableID) []TableID {
	if len(b) == 0 {
		return a
	}
	if len(a) == 0 {
		return b
	}
	// both sets are not empty, find an intersection
	resultSet := set.New[TableID]()
	for iA := range a {
		for iB := range b {
			if a[iA].Includes(b[iB]) {
				resultSet.Add(b[iB])
			} else if b[iB].Includes(a[iA]) {
				resultSet.Add(a[iA])
			}
		}
	}
	return resultSet.SortedSliceFunc(func(a, b TableID) bool {
		return a.Less(b) < 0
	})
}

// Storage is for simple storage implementations
// For extra functionalities implement below storages.
type Storage interface {
	Closeable

	Ping() error
	LoadTable(ctx context.Context, table TableDescription, pusher Pusher) error
	TableSchema(ctx context.Context, table TableID) (*TableSchema, error)
	TableList(filter IncludeTableList) (TableMap, error)

	ExactTableRowsCount(table TableID) (uint64, error)
	EstimateTableRowsCount(table TableID) (uint64, error)
	TableExists(table TableID) (bool, error)
}

// PositionalStorage some storages may provide specific position for snapshot consistency
type PositionalStorage interface {
	// Position provide info about snapshot read position
	Position(ctx context.Context) (*LogPosition, error)
}

type LogPosition struct {
	ID   uint32
	LSN  uint64
	TxID string
}

// SchemaStorage allow to resolve DB Schema from storage
type SchemaStorage interface {
	LoadSchema() (DBSchema, error)
}

// SampleableStorage is for dataplane tests
type SampleableStorage interface {
	Storage

	TableSizeInBytes(table TableID) (uint64, error)
	LoadTopBottomSample(table TableDescription, pusher Pusher) error
	LoadRandomSample(table TableDescription, pusher Pusher) error
	LoadSampleBySet(table TableDescription, keySet []map[string]interface{}, pusher Pusher) error
	TableAccessible(table TableDescription) bool
}

// ShardingStorage is for in table sharding
type ShardingStorage interface {
	ShardTable(ctx context.Context, table TableDescription) ([]TableDescription, error)
}

// Storage has data, that need to be shared with all workers
type ShardingContextStorage interface {
	// ShardingContext Return shared data, used on *MAIN* worker;
	// Take care, method return OperationState_ShardedUploadState, but only fill field Context;
	// Because type of Context is private, this is protoc thing;
	ShardingContext() ([]byte, error)

	// SetShardingContext for storage, used on *SECONDARY* worker
	SetShardingContext(shardedState []byte) error
}

type IncrementalStorage interface {
	GetNextIncrementalState(ctx context.Context, incremental []IncrementalTable) ([]IncrementalState, error)
	BuildArrTableDescriptionWithIncrementalState(tables []TableDescription, incremental []IncrementalTable) []TableDescription
}

type SnapshotableStorage interface {
	BeginSnapshot(ctx context.Context) error
	EndSnapshot(ctx context.Context) error
}
