package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type NotMasterError struct {
	connParams *ConnectionParams
}

func (e NotMasterError) Is(err error) bool {
	_, ok := err.(NotMasterError)
	return ok
}

func (e NotMasterError) Error() string {
	return fmt.Sprintf("Storage %v:%v is not master", e.connParams.Host, e.connParams.Port)
}

type Storage struct {
	ConnectionParams        *ConnectionParams
	useFakePrimaryKey       bool
	degreeOfParallelism     int
	fqtnSchema              map[abstract.TableID]*abstract.TableSchema
	snapshotConnectionsPool chan *sql.Conn
	tableFilter             abstract.Includeable
	IsHomo                  bool
	DB                      *sql.DB
	preSteps                *MysqlDumpSteps
	consistentSnapshot      bool
}

func (s *Storage) Close() {
	if err := s.DB.Close(); err != nil {
		logger.Log.Errorf("Unable to close database: %v", err)
	}
}

func (s *Storage) Ping() error {
	return s.DB.Ping()
}

func (s *Storage) DatabaseSchema() string {
	return s.ConnectionParams.Database
}

func (s *Storage) BeginSnapshot(ctx context.Context) error {
	if !s.consistentSnapshot {
		return nil
	}
	if s.snapshotConnectionsPool != nil {
		return xerrors.New("Snapshot already initiated")
	}

	lockConnection, err := s.DB.Conn(ctx)
	if err != nil {
		return xerrors.Errorf("Can't create connection: %w", err)
	}

	if _, err := lockConnection.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK;"); err != nil {
		return xerrors.Errorf("Can't lock tables: %w", err)
	}

	var rollbacks util.Rollbacks
	rollbacks.Add(func() {
		// Close transactions
		select {
		case connection, ok := <-s.snapshotConnectionsPool:
			if ok {
				if err := connection.Close(); err != nil {
					logger.Log.Error("Can't close connection", log.Error(err))
				}
			}
		default:
		}
		close(s.snapshotConnectionsPool)
		s.snapshotConnectionsPool = nil

		// Unlock tables
		if _, err := lockConnection.ExecContext(ctx, "UNLOCK TABLES;"); err != nil {
			logger.Log.Error("Can't unlock tables", log.Error(err))
		}
	})

	s.snapshotConnectionsPool = make(chan *sql.Conn, s.degreeOfParallelism)
	for i := 0; i < s.degreeOfParallelism; i++ {
		snapshotConnection, err := s.DB.Conn(ctx)
		if err != nil {
			rollbacks.Do()
			return xerrors.Errorf("Can't create connection: %w", err)
		}

		_, err = snapshotConnection.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
		if err != nil {
			if err := snapshotConnection.Close(); err != nil {
				logger.Log.Error("Can't close connection", log.Error(err))
			}
			rollbacks.Do()
			return xerrors.Errorf("Can't create connection: %w", err)
		}

		s.snapshotConnectionsPool <- snapshotConnection
	}

	if _, err := lockConnection.ExecContext(ctx, "UNLOCK TABLES;"); err != nil {
		rollbacks.Do()
		return xerrors.Errorf("Can't unlock tables: %w", err)
	}

	if err := lockConnection.Close(); err != nil {
		rollbacks.Do()
		return xerrors.Errorf("Can't close connection: %w", err)
	}

	rollbacks.Cancel()
	return nil
}

func (s *Storage) EndSnapshot(context.Context) error {
	if !s.consistentSnapshot {
		return nil
	}
	count := 0
	for connection := range s.snapshotConnectionsPool {
		if err := connection.Close(); err != nil {
			return xerrors.Errorf("Can't close connection: %w", err)
		}

		count++
		if count == s.degreeOfParallelism {
			break
		}
	}
	close(s.snapshotConnectionsPool)
	s.snapshotConnectionsPool = nil

	return nil
}

type Queryable interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

func (s *Storage) getSnapshotQueryable(ctx context.Context) (Queryable, *util.Rollbacks, error) {
	var rollbacks util.Rollbacks

	if s.snapshotConnectionsPool != nil {
		// For consistent snapshot

		snapshotConnection, ok := <-s.snapshotConnectionsPool
		if !ok {
			return nil, nil, xerrors.New("Snapshot is ended")
		}
		rollbacks.Add(func() {
			s.snapshotConnectionsPool <- snapshotConnection
		})
		return snapshotConnection, &rollbacks, nil
	} else {
		// For usual snapshot

		connection, err := s.DB.Conn(ctx)
		if err != nil {
			return nil, nil, xerrors.Errorf("can't create connection: %w", err)
		}
		transaction, err := connection.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			if err := connection.Close(); err != nil {
				logger.Log.Error("Can't close connection", log.Error(err))
			}
			return nil, nil, err
		}
		rollbacks.Add(func() {
			if err := transaction.Rollback(); err != nil {
				logger.Log.Error("Can't rollback transaction", log.Error(err))
			}
			if err := connection.Close(); err != nil {
				logger.Log.Error("Can't close connection", log.Error(err))
			}
		})
		return transaction, &rollbacks, nil
	}
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.fqtnSchema[table], nil
}

func (s *Storage) Position(ctx context.Context) (*abstract.LogPosition, error) {
	tx, rollbacks, err := s.getSnapshotQueryable(ctx)
	if err != nil {
		return nil, xerrors.Errorf("Can't get table read transaction: %w", err)
	}
	defer rollbacks.Do()

	isSlave := false
	file, pos, err := s.getBinlogPosition(ctx, tx)
	if err != nil {
		notMaster := new(NotMasterError)
		if xerrors.As(err, &notMaster) || xerrors.As(err, notMaster) {
			isSlave = true
			logger.Log.Info(notMaster.Error())
			err = nil
		} else {
			return nil, xerrors.Errorf("unable to get binlog position: %w", err)
		}
	}
	var lsn uint64
	var gtid string
	var txSequence uint32

	if !isSlave {
		lsn = CalculateLSN(file, pos)
		txSequence = uint32(lsn)
		gtid, err = s.getGtid(ctx, tx)
		if err != nil {
			return nil, xerrors.Errorf("unable to get gtid: %w", err)
		}

		if gtid != "" {
			hash := fnv.New32()
			_, _ = hash.Write([]byte(gtid))
			txSequence = hash.Sum32()
		}
	}
	return &abstract.LogPosition{
		ID:   txSequence,
		LSN:  lsn,
		TxID: gtid,
	}, nil
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	st := util.GetTimestampFromContextOrNow(ctx)

	pos, err := s.Position(ctx)
	if err != nil {
		return xerrors.Errorf("unable to read log position: %w", err)
	}

	tx, rollbacks, err := s.getSnapshotQueryable(ctx)
	if err != nil {
		return xerrors.Errorf("Can't get table read transaction: %w", err)
	}
	defer rollbacks.Do()

	currTableSchema := s.fqtnSchema[table.ID()]

	querySelect := buildSelectQuery(table, currTableSchema.Columns())

	logger.Log.Info("Storage read table", log.String("table", table.Fqtn()), log.String("query", querySelect))

	if _, err := tx.ExecContext(ctx, "set net_read_timeout=3600;"); err != nil {
		return xerrors.Errorf("Unable to set net_read_timeout=3600: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "set net_write_timeout=3600;"); err != nil {
		return xerrors.Errorf("Unable to set net_write_timeout=3600: %w", err)
	}
	timezone := timezoneOffset(s.ConnectionParams.Location)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("set time_zone = '%s';", timezone)); err != nil {
		return xerrors.Errorf("unable to set session timezone %s: %w", timezone, err)
	}

	tableRowsCount, tableDataSizeInBytes, err := getTableRowsCountTableDataSizeInBytes(ctx, tx, table)
	if err != nil {
		return xerrors.Errorf("Unable to get table %v size: %w", table.Fqtn(), err)
	}

	chunkSize := calcChunkSize(tableRowsCount, tableDataSizeInBytes, 100000)

	logger.Log.Infof("Table %v: total rows %v, size %v in chunks by %v", table.Fqtn(), tableRowsCount, humanize.BigBytes(big.NewInt(int64(tableDataSizeInBytes))), chunkSize)

	if table.Offset == 0 && table.Filter == "" && s.preSteps.Tables && s.IsHomo {
		if err := pushCreateTable(ctx, tx, table.ID(), st, pusher); err != nil {
			return xerrors.Errorf("Unable to push drop and create table %v DDL: %w", table.Fqtn(), err)
		}
	}

	colNameToColTypeName, err := makeMapColNameToColTypeName(ctx, tx, table.Name)
	if err != nil {
		return xerrors.Errorf("unable to get column types: %w", err)
	}

	rows, err := tx.QueryContext(ctx, querySelect)
	if err != nil {
		logger.Log.Error("rows select error", log.Error(err))
		return xerrors.Errorf("Unable to select data from table %v: %w", table.Fqtn(), err)
	}
	defer rows.Close()

	err = readRowsAndPushByChunks(
		s.ConnectionParams.Location,
		rows,
		st,
		table,
		currTableSchema,
		colNameToColTypeName,
		chunkSize,
		pos.LSN,
		s.IsHomo,
		pusher,
	)
	if err != nil {
		return xerrors.Errorf("unable to read rows and push by chunks: %w", err)
	}

	err = rows.Err()
	if err != nil {
		msg := "Unable to read rows"
		logger.Log.Warn(msg, log.Error(err))
		return xerrors.Errorf("%v: %w", msg, err)
	}
	logger.Log.Info("Done read rows")
	return nil
}

func (s *Storage) getBinlogPosition(ctx context.Context, tx Queryable) (string, uint64, error) {
	masterStatusQuery := "show master status;"
	_, version, err := CheckMySQLVersion(s)
	if err != nil {
		return "", 0, xerrors.Errorf("unable to check MySQL version: %w", err)
	}

	if strings.HasPrefix(version, "8.4") {
		masterStatusQuery = " SHOW BINARY LOG STATUS;"
	}

	binlogStatus, err := tx.QueryContext(ctx, masterStatusQuery)
	if IsErrorCode(err, ErrCodeSyntax) {
		logger.Log.Warn("master status failed with `1064`-code, probably protocol mismatch, retry with `show replication status;`")
		// for single-store compatibility, see: https://docs.singlestore.com/cloud/reference/sql-reference/operational-commands/show-replication-status/
		binlogStatus, err = tx.QueryContext(ctx, "show replication status;")
	}
	if err != nil {
		return "", 0, xerrors.Errorf("unable to get master status: %w", err)
	}
	var file string
	var pos uint64
	filesCount := 0
	for binlogStatus.Next() {
		filesCount++
		colsNames, err := binlogStatus.Columns()
		if err != nil {
			return "", 0, xerrors.Errorf("unable to get binlog column names: %w", err)
		}

		cols := make([]interface{}, len(colsNames))
		colPtrs := make([]interface{}, len(colsNames))
		for i := 0; i < len(colsNames); i++ {
			colPtrs[i] = &cols[i]
		}
		if err := binlogStatus.Scan(colPtrs...); err != nil {
			return "", 0, xerrors.Errorf("unable to scan binlog columns: %w", err)
		}
		for i, col := range colsNames {
			if strings.ToLower(col) == "file" {
				if v, ok := cols[i].([]byte); ok {
					file = string(v)
				}
			}
			if strings.ToLower(col) == "position" {
				if v, ok := cols[i].(uint64); ok {
					pos = v
				}
			}
		}
	}
	if filesCount == 0 {
		logger.Log.Info("there is not master: show master status returns empty result")
		return file, pos, &NotMasterError{connParams: s.ConnectionParams}
	}
	logger.Log.Infof("Snapshot position: %v:%v", file, pos)
	return file, pos, nil
}

func (s *Storage) ListViews() ([]abstract.TableID, error) {
	rows, err := s.DB.Query(`
                SELECT
                        table_schema,
                        table_name
                FROM information_schema.tables
                WHERE
                        table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                        AND (table_schema = ? || ? = '*')
                        AND (table_type = 'VIEW')
                ORDER BY
                        table_schema,
                        table_name
        `, s.ConnectionParams.Database, s.ConnectionParams.Database)
	if err != nil {
		return nil, xerrors.Errorf("unable to select views info: %w", err)
	}
	defer rows.Close()

	views := make([]abstract.TableID, 0)
	for rows.Next() {
		var tID abstract.TableID
		if err := rows.Scan(&tID.Namespace, &tID.Name); err != nil {
			return nil, xerrors.Errorf("unable to scan view info row: %w", err)
		}
		views = append(views, tID)
	}
	return views, nil
}

func (s *Storage) TableList(includeTableFilter abstract.IncludeTableList) (abstract.TableMap, error) {
	warnTooLongExec := util.DelayFunc(
		func() {
			logger.Log.Warn("Schema retrieval takes longer than usual. Check the list of tables included in the transfer and the load of source database.")
		},
		5*time.Minute,
	)
	defer warnTooLongExec.Cancel()

	rows, err := s.DB.Query(`
		SELECT
			table_schema,
			table_name,
			COALESCE(table_rows, 0),
            table_type = 'VIEW' as is_view
		FROM information_schema.tables
		WHERE
			table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
			AND (table_schema = ? || ? = '')
			AND (table_type in ('BASE TABLE', 'VIEW'))
		ORDER BY
			table_schema,
			table_name
	`, s.ConnectionParams.Database, s.ConnectionParams.Database)
	if err != nil {
		return nil, xerrors.Errorf("unable to select tables info: %w", err)
	}
	defer rows.Close()

	tables := make(abstract.TableMap)
	for rows.Next() {
		var tID abstract.TableID
		var tInfo abstract.TableInfo
		if err := rows.Scan(&tID.Namespace, &tID.Name, &tInfo.EtaRow, &tInfo.IsView); err != nil {
			return nil, xerrors.Errorf("unable to scan table info row: %w", err)
		}

		if isSystemTable(tID.Name) {
			continue
		}
		if s.IsHomo && tInfo.IsView {
			continue
		}

		tables[tID] = tInfo
	}

	schema, err := s.LoadSchema()
	if err != nil {
		return nil, xerrors.Errorf("Cannot load schema: %w", err)
	}

	result := make(abstract.TableMap)
	for tID, tInfo := range tables {
		columns, ok := schema[tID]
		if !ok {
			if s.tableFilter != nil && !s.tableFilter.Include(tID) {
				continue
			}
			return nil, xerrors.Errorf("Table schema not found for table %s (can be result of lacking some grants)", tID.Fqtn())
		}
		tInfo.Schema = columns
		result[tID] = tInfo
	}

	return model.FilteredMap(result, includeTableFilter), nil
}

func (s *Storage) LoadSchema() (schema abstract.DBSchema, err error) {
	return LoadSchema(s.DB, s.useFakePrimaryKey, true)
}

func (s *Storage) getGtid(ctx context.Context, tx Queryable) (string, error) {
	var gtidSet string
	err := tx.QueryRowContext(ctx, "select @@global.gtid_executed;").Scan(&gtidSet)
	if err != nil {
		if strings.Contains(err.Error(), "Unknown system variable 'gtid_executed'") {
			return "", nil
		}
		return "", xerrors.Errorf("Unable to get gtid executed: %w", err)
	}
	return gtidSet, nil
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return 0, xerrors.New("not implemented")
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	row := s.DB.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM `%s`.`%s`;", table.Namespace, table.Name))

	var rowsCount uint64
	if err := row.Scan(&rowsCount); err != nil {
		return 0, xerrors.Errorf("unable to count table rows:%w", err)
	}
	return rowsCount, nil
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = '%s'
			AND table_name = '%s'
		LIMIT 1;
	`, table.Namespace, table.Name)

	row := s.DB.QueryRow(query)

	var rowsCount uint64
	if err := row.Scan(&rowsCount); err != nil {
		return false, xerrors.Errorf("unable to count table rows:%w", err)
	}
	return rowsCount != 0, nil
}

func timezoneOffset(timezone *time.Location) string {
	_, secondsOffset := time.Now().In(timezone).Zone()
	sign := "+"
	if secondsOffset < 0 {
		sign = "-"
		secondsOffset *= -1
	}

	minutesOffset := secondsOffset / 60
	hours := strconv.Itoa(minutesOffset / 60)
	if len(hours) == 1 {
		hours = "0" + hours
	}
	minutes := strconv.Itoa(minutesOffset % 60)
	if len(minutes) == 1 {
		minutes = "0" + minutes
	}
	return fmt.Sprintf("%s%s:%s", sign, hours, minutes)
}

func NewStorage(config *MysqlStorageParams) (*Storage, error) {
	var rollbacks util.Rollbacks
	defer rollbacks.Do()

	connectionParams, err := NewConnectionParams(config)
	if err != nil {
		return nil, xerrors.Errorf("Can't connect to server: %w", err)
	}

	db, err := Connect(connectionParams, nil)
	if err != nil {
		return nil, xerrors.Errorf("Can't connect to server: %w", err)
	}
	rollbacks.AddCloser(db, logger.Log, "cannot close database")

	fqtnToSchema, err := LoadSchema(db, config.UseFakePrimaryKey, true)
	if err != nil {
		return nil, xerrors.Errorf("Can't load schema: %w", err)
	}

	rollbacks.Cancel()
	storage := &Storage{
		ConnectionParams:        connectionParams,
		useFakePrimaryKey:       config.UseFakePrimaryKey,
		degreeOfParallelism:     config.DegreeOfParallelism,
		consistentSnapshot:      config.ConsistentSnapshot,
		fqtnSchema:              fqtnToSchema,
		tableFilter:             config.TableFilter,
		preSteps:                config.PreSteps,
		DB:                      db,
		snapshotConnectionsPool: nil,
		IsHomo:                  false,
	}

	return storage, nil
}
