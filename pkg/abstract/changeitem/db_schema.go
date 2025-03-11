package changeitem

import (
	"encoding/json"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/util"
)

type DBSchema map[TableID]*TableSchema

type includeable interface {
	// Include returns true if the given table is included
	Include(tID TableID) bool
}

func (s *DBSchema) String(withSchema bool, filter includeable) string {
	mapCopy := make(map[string]*TableSchema)
	for k, v := range *s {
		if filter != nil && !filter.Include(k) {
			continue
		}
		var newTableInfo *TableSchema
		if withSchema {
			newTableInfo = v
		}
		mapCopy[k.Fqtn()] = newTableInfo
	}
	result, _ := json.Marshal(mapCopy)
	return string(result)
}

func (s DBSchema) CheckPrimaryKeys(filter includeable) error {
	var errs util.Errors
	for tID, columns := range s {
		if !filter.Include(tID) {
			continue
		}
		if !columns.Columns().HasPrimaryKey() {
			errs = append(errs, xerrors.Errorf("%s: no key columns found", tID.Fqtn()))
		}
	}
	if len(errs) > 0 {
		return xerrors.Errorf("Tables: %v / %v check failed:\n%w", len(errs), len(s), errs)
	}
	return nil
}

func (s DBSchema) FakePkeyTables(filter includeable) []TableID {
	var fakePkeyTables []TableID
	for tID, columns := range s {
		if !filter.Include(tID) {
			continue
		}
		if columns.Columns().HasFakeKeys() {
			fakePkeyTables = append(fakePkeyTables, tID)
		}
	}
	return fakePkeyTables
}
