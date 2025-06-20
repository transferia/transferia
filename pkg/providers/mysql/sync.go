//go:build !disable_mysql_provider

package mysql

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/gofrs/uuid"
	"github.com/pingcap/parser/ast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func (c *Canal) startSyncer() (*replication.BinlogStreamer, error) {
	gset := c.master.GTIDSet()
	if gset == nil || gset.String() == "" {
		pos := c.master.Position()
		s, err := c.syncer.StartSync(pos)
		if err != nil {
			return nil, xerrors.Errorf("failed to start sync replication at binlog %v: %w", pos, err)
		}
		c.logger.Infof("start sync binlog at binlog file %v", pos)
		return s, nil
	} else {
		gsetClone := gset.Clone()
		s, err := c.syncer.StartSyncGTID(gset)
		if err != nil {
			return nil, xerrors.Errorf("failed start sync replication at GTID set %v: %w", gset, err)
		}
		c.logger.Infof("start sync binlog at GTID set %v", gsetClone)
		return s, nil
	}
}

func (c *Canal) runSyncBinlog() error {
	s, err := c.startSyncer()
	if err != nil {
		return xerrors.Errorf("failed to start syncer: %w", err)
	}

	savePos := false
	force := false

	// The name of the binlog file received in the fake rotate event.
	// It must be preserved until the new position is saved.
	fakeRotateLogName := ""

	// query may be filled if binlog_rows_query_log_events option is enabled
	rowsQuery := ""

	initPos := c.master.Position()
	c.logger.Info("setting initial position", log.String("File", initPos.Name), log.UInt32("Position", initPos.Pos))
	if err := c.eventHandler.OnPosSynced(initPos, c.master.GTIDSet(), true); err != nil {
		return xerrors.Errorf("failed while syncing position: %w", err)
	}

	for {
		ev, err := s.GetEvent(c.ctx)
		cErr := util.Unwrap(err)
		var mErr *mysql.MyError
		if xerrors.As(cErr, &mErr) {
			if mErr.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG {
				c.logger.Error("failed to get canal event (fatal)", log.Error(mErr))
				return xerrors.Errorf("failed to get canal event (fatal): %w", abstract.NewFatalError(err))
			}
		}

		if err != nil {
			return xerrors.Errorf("failed to get canal event: %w", err)
		}

		// Update the delay between the Canal and the Master before the handler hooks are called
		c.updateReplicationDelay(ev)

		// If log pos equals zero then the received event is a fake rotate event and
		// contains only a name of the next binlog file
		// See https://github.com/mysql/mysql-server/blob/8e797a5d6eb3a87f16498edcb7261a75897babae/sql/rpl_binlog_sender.h#L235
		// and https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/rpl_binlog_sender.cc#L899
		if ev.Header.LogPos == 0 {
			switch e := ev.Event.(type) {
			case *replication.RotateEvent:
				fakeRotateLogName = string(e.NextLogName)
				c.logger.Infof("received fake rotate event, next log name is %s", e.NextLogName)
			}

			continue
		}

		savePos = false
		force = false
		pos := c.master.Position()

		curPos := pos.Pos
		// next binlog pos
		pos.Pos = ev.Header.LogPos

		// new file name received in the fake rotate event
		if fakeRotateLogName != "" {
			pos.Name = fakeRotateLogName
		}

		// We only save position with RotateEvent and XIDEvent.
		// For RowsEvent, we can't save the position until meeting XIDEvent
		// which tells the whole transaction is over.
		// TODO: If we meet any DDL query, we must save too.
		switch event := ev.Event.(type) {
		case *replication.RotateEvent:
			pos.Name = string(event.NextLogName)
			pos.Pos = uint32(event.Position)
			c.logger.Infof("rotate binlog to %s", pos)
			savePos = true
			force = true
			if err = c.eventHandler.OnRotate(event); err != nil {
				return xerrors.Errorf("rotation handler failed: %w", err)
			}
		case *replication.RowsQueryEvent:
			// A RowsQueryEvent will always precede the corresponding RowsEvent
			rowsQuery = string(event.Query)
			continue
		case *replication.RowsEvent:
			// we only focus row based event
			err = c.handleRowsEvent(ev, rowsQuery)
			if err != nil {
				isFailure := !(xerrors.Is(err, ErrExcludedTable) || xerrors.Is(err, schema.ErrTableNotExist) || xerrors.Is(err, schema.ErrMissingTableMeta))
				if isFailure {
					c.logger.Errorf("handle rows event at (%s, %d) error %v", pos.Name, curPos, err)
					return xerrors.Errorf("failed to handle row event: %w", err)
				}
			}
			continue
		case *replication.XIDEvent:
			rowsQuery = ""
			savePos = true
			// try to save the position later
			if err := c.eventHandler.OnXID(pos); err != nil {
				return xerrors.Errorf("OnXID handler failed: %w", err)
			}
			if event.GSet != nil {
				c.master.UpdateGTIDSet(event.GSet)
			}
		case *replication.MariadbGTIDEvent:
			rowsQuery = ""
			// try to save the GTID later
			gtid, err := mysql.ParseMariadbGTIDSet(event.GTID.String())
			if err != nil {
				return xerrors.Errorf("MariaDB GTID set parsing failed: %w", err)
			}
			if err := c.eventHandler.OnGTID(gtid); err != nil {
				return xerrors.Errorf("OnGTID MariaDB handler failed: %w", err)
			}
		case *replication.GTIDEvent:
			rowsQuery = ""
			u, _ := uuid.FromBytes(event.SID)
			gtid, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), event.GNO))
			if err != nil {
				return xerrors.Errorf("MySQL GTID set parsing failed: %w", err)
			}
			if err := c.eventHandler.OnGTID(gtid); err != nil {
				return xerrors.Errorf("OnGTID MySQL handler failed: %w", err)
			}
		case *replication.QueryEvent:
			stmts, _, err := c.parser.Parse(string(event.Query), "", "")
			if err != nil {
				c.logger.Errorf("parse query(%s) err %v, will skip this event", event.Query, err)
				continue
			}
			for _, stmt := range stmts {
				stmntNodes := parseStmt(stmt)
				includedNodes := make([]*node, 0)
				for _, node := range stmntNodes {
					if node.db == "" {
						node.db = string(event.Schema)
					}
					if c.checkTableMatch(node.db, node.table) {
						includedNodes = append(includedNodes, node)
						if err = c.updateTable(node.db, node.table); err != nil {
							return xerrors.Errorf("failed to process table update: %w", err)
						}
					}
				}
				if len(includedNodes) > 0 {
					savePos = true
					force = true
					// Now we only handle Table Changed DDL, maybe we will support more later.
					if err = c.eventHandler.OnDDL(pos, event); err != nil {
						return xerrors.Errorf("OnDDL handler failed: %w", err)
					}
				}
			}
			if savePos && event.GSet != nil {
				c.master.UpdateGTIDSet(event.GSet)
			}
		default:
			continue
		}

		if savePos {
			c.master.Update(pos)
			c.master.UpdateTimestamp(ev.Header.Timestamp)
			fakeRotateLogName = ""

			if err := c.eventHandler.OnPosSynced(pos, c.master.GTIDSet(), force); err != nil {
				return xerrors.Errorf("OnPosSynced handler failed: %w", err)
			}
		}
	}
}

type node struct {
	db    string
	table string
}

func parseStmt(stmt ast.StmtNode) (ns []*node) {
	switch t := stmt.(type) {
	case *ast.RenameTableStmt:
		for _, tableInfo := range t.TableToTables {
			n := &node{
				db:    tableInfo.OldTable.Schema.String(),
				table: tableInfo.OldTable.Name.String(),
			}
			ns = append(ns, n)
		}
	case *ast.AlterTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.DropTableStmt:
		for _, table := range t.Tables {
			n := &node{
				db:    table.Schema.String(),
				table: table.Name.String(),
			}
			ns = append(ns, n)
		}
	case *ast.CreateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.TruncateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Schema.String(),
		}
		ns = []*node{n}
	}
	return ns
}

func (c *Canal) updateTable(db, table string) (err error) {
	c.ClearTableCache([]byte(db), []byte(table))
	c.logger.Infof("table structure changed, clear table cache: %s.%s\n", db, table)
	if err = c.eventHandler.OnTableChanged(db, table); err != nil && !xerrors.Is(err, schema.ErrTableNotExist) {
		return xerrors.Errorf("OnTableChanged handler failed: %w", err)
	}
	return err
}

func (c *Canal) updateReplicationDelay(ev *replication.BinlogEvent) {
	var newDelay uint32
	now := uint32(time.Now().Unix())
	if now >= ev.Header.Timestamp {
		newDelay = now - ev.Header.Timestamp
	}
	atomic.StoreUint32(c.delay, newDelay)
}

func (c *Canal) handleRowsEvent(e *replication.BinlogEvent, rowsQuery string) error {
	rowsEvent := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	schemaName := string(rowsEvent.Table.Schema)
	tableName := string(rowsEvent.Table.Table)
	table, err := c.GetTable(schemaName, tableName)
	if err != nil {
		return xerrors.Errorf("failed to get table: %w", err)
	}
	c.logger.Debugf("event %v.%v %v %v", schemaName, tableName, e.Header.EventType, err)
	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = UpdateAction
	default:
		return xerrors.Errorf("%q is not supported now", e.Header.EventType)
	}
	event := newRowsEvent(table, action, e.Header, rowsEvent, rowsQuery)
	if err := Validate(event); err != nil {
		return xerrors.Errorf("Validation failed for row event: %w", err)
	}
	if err := RestoreStringAndBytes(event); err != nil {
		return xerrors.Errorf("Failed to restore data in row event: %w", err)
	}
	return c.eventHandler.OnRow(event)
}

func (c *Canal) FlushBinlog() error {
	_, err := c.Execute("FLUSH BINARY LOGS")
	if err != nil {
		return xerrors.Errorf("failed to execute SQL \"FLUSH BINARY LOGS\": %w", err)
	}
	return nil
}

func (c *Canal) WaitUntilPos(pos mysql.Position, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			return xerrors.Errorf("wait position %v too long > %s", pos, timeout)
		default:
			err := c.FlushBinlog()
			if err != nil {
				return xerrors.Errorf("failed to flush binary logs: %w", err)
			}
			curPos := c.master.Position()
			if curPos.Compare(pos) >= 0 {
				return nil
			} else {
				c.logger.Debugf("master pos is %v, wait catching %v", curPos, pos)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (c *Canal) GetMasterPos() (mysql.Position, error) {
	rr, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{}, xerrors.Errorf("failed to execute SQL \"SHOW MASTER STATUS\": %w", err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return mysql.Position{Name: name, Pos: uint32(pos)}, nil
}

func (c *Canal) GetMasterGTIDSet() (mysql.GTIDSet, error) {
	query := ""
	switch c.cfg.Flavor {
	case mysql.MariaDBFlavor:
		query = "SELECT @@GLOBAL.gtid_current_pos"
	default:
		query = "SELECT @@GLOBAL.GTID_EXECUTED"
	}
	rr, err := c.Execute(query)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute GTID retrieval query: %w", err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, xerrors.Errorf("failed to get GTID string from resultset: %w", err)
	}
	gset, err := mysql.ParseGTIDSet(c.cfg.Flavor, gx)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse GTID set: %w", err)
	}
	return gset, nil
}

func (c *Canal) CatchMasterPos(timeout time.Duration) error {
	pos, err := c.GetMasterPos()
	if err != nil {
		return xerrors.Errorf("failed to obtain master position: %w", err)
	}

	return c.WaitUntilPos(pos, timeout)
}
