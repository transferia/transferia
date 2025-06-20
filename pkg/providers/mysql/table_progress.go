//go:build !disable_mysql_provider

package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type LsnProgress interface {
	GetCurrentState() (map[string]TableStatus, error)
	BuildLSNQuery(name string, lsn uint64, status Status) string
}

type TableProgress struct {
	db     *sql.DB
	dbName string
}
type Status string

const (
	SnapshotWait = Status("SnapshotWait")
	SyncWait     = Status("SyncWait")
	InSync       = Status("InSync")
)

type TableStatus struct {
	LSN    uint64
	Status Status
}

func (t *TableProgress) GetCurrentState() (map[string]TableStatus, error) {
	rows, err := t.db.Query(fmt.Sprintf("select name, lsn, status from %v.__table_transfer_progress", t.dbName))
	if err != nil {
		return nil, xerrors.Errorf("unable to select table transfer progress: %w", err)
	}
	res := map[string]TableStatus{}
	defer rows.Close()
	for rows.Next() {
		var name, status string
		var lsn uint64
		if err := rows.Scan(&name, &lsn, &status); err != nil {
			return nil, xerrors.Errorf("unable to scan table transfer progress row: %w", err)
		}
		res[name] = TableStatus{
			LSN:    lsn,
			Status: Status(status),
		}
	}
	return res, nil
}

func (t *TableProgress) BuildLSNQuery(name string, lsn uint64, status Status) string {
	return fmt.Sprintf(`
replace into %v.__table_transfer_progress (name, lsn, status) values ('%v', %v, '%v');
`, t.dbName, name, lsn, status)
}

func (t *TableProgress) init() error {
	_, err := t.db.Exec(fmt.Sprintf(`
create table if not exists %v.__table_transfer_progress (
	name varchar(255) primary key ,
	lsn bigint,
	status enum ('SnapshotWait', 'SyncWait', 'InSync')
);`, t.dbName))
	return err
}

func NewTableProgressTracker(db *sql.DB, dbName string) (LsnProgress, error) {
	t := &TableProgress{
		db:     db,
		dbName: dbName,
	}
	if !strings.HasPrefix(t.dbName, "`") && !strings.HasSuffix(t.dbName, "`") {
		t.dbName = fmt.Sprintf("`%v`", t.dbName)
	}
	if err := t.init(); err != nil {
		return nil, err
	}

	return t, nil
}

type EmptyProgress struct{}

func (t *EmptyProgress) GetCurrentState() (map[string]TableStatus, error) {
	res := map[string]TableStatus{}
	return res, nil
}

func (t *EmptyProgress) BuildLSNQuery(name string, lsn uint64, status Status) string {
	return "select 42;"
}

func NewEmptyProgress() (LsnProgress, error) {
	return &EmptyProgress{}, nil
}
