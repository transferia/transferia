package logtracker

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

type EmbeddedLogTracker struct {
	sqlxDB     *sqlx.DB
	config     *provider_oracle.OracleSource
	transferID string
}

func NewEmbeddedLogTracker(sqlxDB *sqlx.DB, config *provider_oracle.OracleSource, transferID string) (*EmbeddedLogTracker, error) {
	tracker := &EmbeddedLogTracker{
		sqlxDB:     sqlxDB,
		config:     config,
		transferID: transferID,
	}
	return tracker, nil
}

func (tracker *EmbeddedLogTracker) TransferID() string {
	return tracker.transferID
}

func (tracker *EmbeddedLogTracker) initCore(ctx context.Context, connection *sqlx.Conn) error {
	checkSQL := "select count(*) from ALL_TABLES where OWNER = upper(:user_name) and TABLE_NAME = upper(:table_name)"
	var count int
	if err := connection.GetContext(ctx, &count, checkSQL, tracker.config.User, oracle_common.EmbeddedLogTrackerTableName); err != nil {
		return xerrors.Errorf("Can't check tracker table: %w", err)
	}
	if count != 0 {
		return nil
	}

	createSQL := fmt.Sprintf(`
create table %[1]v.%[2]v (
	transfer_id varchar2(256) not null,
	scn         number not null,
	rs_id       varchar2(256),
	ssn         number,
	type        varchar2(256) not null,
	ts          date,
	constraint %[2]v_pk primary key (transfer_id)
)`, tracker.config.User, oracle_common.EmbeddedLogTrackerTableName)
	if _, err := connection.ExecContext(ctx, createSQL); err != nil {
		return xerrors.Errorf("Can't create tracker table: %w", err)
	}

	return nil
}

func (tracker *EmbeddedLogTracker) Init() error {
	return oracle_common.PDBQueryGlobal(tracker.config, tracker.sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			return tracker.initCore(ctx, connection)
		})
}

func (tracker *EmbeddedLogTracker) clearPositionCore(ctx context.Context, connection *sqlx.Conn) error {
	sql := fmt.Sprintf("delete from %v.%v where transfer_id = :transfer_id",
		tracker.config.User, oracle_common.EmbeddedLogTrackerTableName)

	if _, err := connection.ExecContext(ctx, sql, tracker.transferID); err != nil {
		return xerrors.Errorf("Can't clear position: %w", err)
	}

	return nil
}

func (tracker *EmbeddedLogTracker) ClearPosition() error {
	return oracle_common.PDBQueryGlobal(tracker.config, tracker.sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			return tracker.clearPositionCore(ctx, connection)
		})
}

type EmbeddedLogPositionRow struct {
	SCN       uint64    `db:"SCN"`
	RSID      *string   `db:"RS_ID"`
	SSN       *uint64   `db:"SSN"`
	Type      string    `db:"TYPE"`
	Timestamp time.Time `db:"TS"`
}

func (tracker *EmbeddedLogTracker) readPositionCore(ctx context.Context, connection *sqlx.Conn) (*oracle_common.LogPosition, error) {
	sql := fmt.Sprintf("select scn, rs_id, ssn, type, ts from %v.%v where transfer_id = :transfer_id",
		tracker.config.User, oracle_common.EmbeddedLogTrackerTableName)

	var row []EmbeddedLogPositionRow
	if err := connection.SelectContext(ctx, &row, sql, tracker.transferID); err != nil {
		return nil, xerrors.Errorf("Can't read position: %w", err)
	}

	if len(row) == 0 {
		return nil, nil
	}

	return oracle_common.NewLogPosition(row[0].SCN, row[0].RSID, row[0].SSN, oracle_common.PositionType(row[0].Type), row[0].Timestamp)
}

func (tracker *EmbeddedLogTracker) ReadPosition() (*oracle_common.LogPosition, error) {
	var position *oracle_common.LogPosition
	queryErr := oracle_common.PDBQueryGlobal(tracker.config, tracker.sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			var readPositionErr error
			position, readPositionErr = tracker.readPositionCore(ctx, connection)
			return readPositionErr
		})
	return position, queryErr
}

func (tracker *EmbeddedLogTracker) writePositionCore(ctx context.Context, connection *sqlx.Conn, position *oracle_common.LogPosition) error {
	transaction, err := connection.BeginTxx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("Transaction begin error, due to insert/update position error: %w", err)
	}

	sql := fmt.Sprintf(`
begin
   insert into %[1]v.%[2]v (transfer_id, scn, rs_id, ssn, type, ts) values (:transfer_id_i, :scn_i, :rs_id_i, :ssn_i, :type_i, :ts_i);
exception
   when dup_val_on_index then
      update %[1]v.%[2]v set scn = :scn_u, rs_id = :rs_id_u, ssn = :ssn_u, type = :type_u, ts = :ts_u where transfer_id = :transfer_id_u;
end;`, tracker.config.User, oracle_common.EmbeddedLogTrackerTableName)

	// Can't use same name parameter twice (-_-)
	_, err = transaction.NamedExecContext(ctx, sql, map[string]interface{}{
		"transfer_id_i": tracker.transferID,
		"scn_i":         position.SCN(),
		"rs_id_i":       position.RSID(),
		"ssn_i":         position.SSN(),
		"type_i":        string(position.Type()),
		"ts_i":          position.Timestamp(),
		"transfer_id_u": tracker.transferID,
		"scn_u":         position.SCN(),
		"rs_id_u":       position.RSID(),
		"ssn_u":         position.SSN(),
		"type_u":        string(position.Type()),
		"ts_u":          position.Timestamp(),
	})

	if err != nil {
		if transactionErr := transaction.Rollback(); transactionErr != nil {
			return xerrors.Errorf("Transaction rollback error: %v, due to insert/update position error: %w", transactionErr.Error(), err)
		}
		return xerrors.Errorf("Can't insert/update position: %w", err)
	}

	if err := transaction.Commit(); err != nil {
		return xerrors.Errorf("Transaction commit error, due to insert/update position error: %w", err)
	}
	return nil
}

func (tracker *EmbeddedLogTracker) WritePosition(position *oracle_common.LogPosition) error {
	return oracle_common.PDBQueryGlobal(tracker.config, tracker.sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			return tracker.writePositionCore(ctx, connection, position)
		})
}
