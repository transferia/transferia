package log_miner

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/abstract2/events"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	"github.com/transferia/transferia/pkg/providers/oracle/logtracker"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	minMineTimeout = time.Second * 5
)

var (
	replicationContinueErr = xerrors.New("Replication is continue, but get error")
	replicationBreakErr    = xerrors.New("Replication is stopped")
	skipErrorCodes         = []string{
		"ORA-12850",
	}
)

type LogType string

const (
	LogTypeOnline   = LogType("Online")
	LogTypeArchived = LogType("Archived")
)

type LogMinerOPCode byte

const (
	LogMinerOPCodeInsert   = LogMinerOPCode(1)
	LogMinerOPCodeDelete   = LogMinerOPCode(2)
	LogMinerOPCodeUpdate   = LogMinerOPCode(3)
	LogMinerOPCodeCommit   = LogMinerOPCode(7)
	LogMinerOPCodeRollback = LogMinerOPCode(36)
)

type oracleLogMinerSource struct {
	sqlxDB     *sqlx.DB
	config     *provider_oracle.OracleSource
	schemaRepo *oracle_schema.Database
	tracker    logtracker.LogTracker
	logger     log.Logger
	metrics    *stats.SourceStats
	run        *oracleLogMinerSourceRun
}

type oracleLogMinerSourceRun struct {
	ctx                     context.Context
	cancel                  context.CancelFunc
	target                  abstract2.EventTarget
	currentLog              *LogFileRow
	startPosition           *oracle_common.LogPosition
	currentPosition         *oracle_common.LogPosition
	currentTimestamp        time.Time
	infoMutex               sync.Mutex
	currentLogReadRowsCount int64
	endOfTheLog             bool
	transactions            *transactionStore
	currentBatch            *logMinerBatch
	batchProcessWait        sync.WaitGroup
	batchProcessError       error
}

func NewLogMinerSource(
	sqlxDB *sqlx.DB,
	config *provider_oracle.OracleSource,
	schema *oracle_schema.Database,
	tracker logtracker.LogTracker,
	logger log.Logger,
	metrics *stats.SourceStats,
) (abstract2.EventSource, error) {
	source := &oracleLogMinerSource{
		sqlxDB:     sqlxDB,
		config:     config,
		schemaRepo: schema,
		tracker:    tracker,
		logger:     logger,
		metrics:    metrics,
		run:        nil,
	}
	return source, nil
}

func (source *oracleLogMinerSource) Running() bool {
	if source.run == nil {
		return false
	}
	return source.run.ctx.Err() == nil
}

func (source *oracleLogMinerSource) createExecuteQuery(procedureCall string) string {
	return fmt.Sprintf("begin\n    %v;\nend;", procedureCall)
}

func (source *oracleLogMinerSource) createExecuteQueries(procedureCalls []string) string {
	builder := strings.Builder{}
	builder.WriteString("begin\n")
	for _, procedureCall := range procedureCalls {
		builder.WriteString(fmt.Sprintf("    %v;\n", procedureCall))
	}
	builder.WriteString("end;")
	return builder.String()
}

type LogFileRow struct {
	ID       uint64 `db:"ID"`
	Type     string `db:"TYPE"`
	FileName string `db:"FILE_NAME"`
	FromSCN  uint64 `db:"FROM_SCN"`
	ToSCN    uint64 `db:"TO_SCN"`
	Bytes    uint64 `db:"BYTES"`
}

func (row *LogFileRow) String() string {
	return fmt.Sprintf("[%v] %v >= SCN %v", row.ID, row.FileName, row.FromSCN)
}

func (source *oracleLogMinerSource) getLogFiles(ctx context.Context, connection *sqlx.Conn, scn uint64) ([]LogFileRow, error) {
	// About THREAD# https://docs.oracle.com/cd/B12037_01/server.101/b10755/initparams211.htm
	sql := `
select * from (
	select ID, TYPE, FILE_NAME, FROM_SCN, TO_SCN, BYTES from (
		select a.SEQUENCE# as ID, 'Archived' as TYPE, a.NAME as FILE_NAME, a.FIRST_CHANGE# as FROM_SCN, a.NEXT_CHANGE# as TO_SCN, NVL(a.BYTES, 0) as BYTES
		from V$ARCHIVED_LOG a
		where a.THREAD# = 1 and a.STATUS = 'A' and a.STANDBY_DEST = 'NO'
		union
		select l.SEQUENCE# as ID, 'Online' as TYPE, f.MEMBER as FILE_NAME, l.FIRST_CHANGE# as FROM_SCN, l.NEXT_CHANGE# as TO_SCN, NVL(l.BYTES, 0) as BYTES
		from V$LOG l
		inner join V$LOGFILE f on f.GROUP# = l.GROUP#
		where l.THREAD# = 1 and l.STATUS in ('CURRENT', 'ACTIVE', 'INACTIVE') and l.ARCHIVED = 'NO' and f.STATUS is null
	)
	where ((:scn >= FROM_SCN and :scn < TO_SCN) or FROM_SCN >= :scn)
	order by FROM_SCN
)
where ROWNUM <= 2`
	var logFiles []LogFileRow
	if err := connection.SelectContext(ctx, &logFiles, sql, scn); err != nil {
		source.logger.Errorf("Can't get log files from DB from scn [%v]", scn)
		return nil, xerrors.Errorf("Can't get log files from DB: %w", err)
	}
	return logFiles, nil
}

func (source *oracleLogMinerSource) addLogFilesToLogMiner(ctx context.Context, connection *sqlx.Conn) error {
	if source.run.currentLog == nil {
		return xerrors.New("Can't add log file to LogMiner: log file is nil")
	}

	procedureCall := fmt.Sprintf("DBMS_LOGMNR.ADD_LOGFILE('%v')", source.run.currentLog.FileName)
	sql := source.createExecuteQuery(procedureCall)
	if _, err := connection.ExecContext(ctx, sql); err != nil {
		source.logger.Errorf("Can't add log '%v' to LogMiner", source.run.currentLog)
		return xerrors.Errorf("Can't add log file to LogMiner: %w", err)
	}

	return nil
}

func (source *oracleLogMinerSource) startLogMiner(ctx context.Context, connection *sqlx.Conn) error {
	options := []string{
		"SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG",
		"SYS.DBMS_LOGMNR.NO_SQL_DELIMITER",
		"SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT",
		"SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT",
	}

	procedureCall := fmt.Sprintf(
		"DBMS_LOGMNR.START_LOGMNR(STARTSCN => %v, OPTIONS => %v)",
		source.run.currentPosition.SCN(), strings.Join(options, "+"))

	sql := source.createExecuteQuery(procedureCall)
	if _, err := connection.ExecContext(ctx, sql); err != nil {
		source.logger.Errorf("Can't start LogMiner position: [%v]", source.run.currentPosition)
		return xerrors.Errorf("Can't start LogMiner: %w", err)
	}

	return nil
}

func (source *oracleLogMinerSource) stopLogMiner(ctx context.Context, connection *sqlx.Conn) error {
	sql := source.createExecuteQuery("DBMS_LOGMNR.END_LOGMNR()")
	if _, err := connection.ExecContext(ctx, sql); err != nil {
		source.logger.Error("Can't stop LogMiner")
		return xerrors.Errorf("Can't stop LogMiner: %w", err)
	}
	return nil
}

func (source *oracleLogMinerSource) parseSQL(row *LogMinerRow, transaction *oracle_common.TransactionInfo) (abstract2.Event, error) {
	parseResult, err := parseLogMinerSQL(row.SQLRedo)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}

	tableID := parseResult.TableID()
	table := source.schemaRepo.OracleTableByID(tableID)
	if table == nil {
		return nil, xerrors.Errorf("Table '%v' not in loaded schema", tableID.OracleSQLName())
	}

	position, err := oracle_common.NewLogPosition(row.SCN, &row.RSID, &row.SSN, oracle_common.PositionReplication, row.Timestamp)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}

	switch opCode := LogMinerOPCode(row.OPCode); opCode {
	case LogMinerOPCodeInsert:
		event := events.NewDefaultTransactionalInsertEvent(table, position, transaction)
		for i := 0; i < table.ColumnsCount(); i++ {
			column := table.OracleColumn(i)
			if valueStr, ok := parseResult.NewValues[column.OracleName()]; ok {
				value, err := castValueFromLogMiner(column, valueStr)
				if err != nil {
					return nil, xerrors.Errorf("Cast value error: %w", err)
				}
				if err := event.AddNewValue(value); err != nil {
					return nil, xerrors.Errorf("Add new value error: %w", err)
				}
			}
		}
		return event, nil
	case LogMinerOPCodeDelete:
		keyExists := false
		event := events.NewDefaultTransactionalDeleteEvent(table, position, transaction)
		for i := 0; i < table.ColumnsCount(); i++ {
			column := table.OracleColumn(i)
			if valueStr, ok := parseResult.OldValues[column.OracleName()]; ok {
				value, err := castValueFromLogMiner(column, valueStr)
				if err != nil {
					return nil, xerrors.Errorf("Cast value error: %w", err)
				}
				if err := event.AddOldValue(value); err != nil {
					return nil, xerrors.Errorf("Add old value error: %w", err)
				}
				if column.Key() {
					keyExists = true
				}
			}
		}
		if !keyExists {
			return nil, xerrors.Errorf("Key value for table '%v' not found, maybe PK supplemental logging need to be enabled",
				table.OracleSQLName())
		}
		return event, nil
	case LogMinerOPCodeUpdate:
		keyExists := false
		event := events.NewDefaultTransactionalUpdateEvent(table, position, transaction)
		for i := 0; i < table.ColumnsCount(); i++ {
			column := table.OracleColumn(i)
			var oldValue abstract2.Value
			if oldValueStr, oldOk := parseResult.OldValues[column.OracleName()]; oldOk {
				oldValue, err = castValueFromLogMiner(column, oldValueStr)
				if err != nil {
					return nil, xerrors.Errorf("Cast value error: %w", err)
				}
				if err := event.AddOldValue(oldValue); err != nil {
					return nil, xerrors.Errorf("Add old value error: %w", err)
				}
				if column.Key() {
					keyExists = true
				}
			}
			if newValueStr, newOk := parseResult.NewValues[column.OracleName()]; newOk {
				newValue, err := castValueFromLogMiner(column, newValueStr)
				if err != nil {
					return nil, xerrors.Errorf("Cast value error: %w", err)
				}
				if err := event.AddNewValue(newValue); err != nil {
					return nil, xerrors.Errorf("Add new value error: %w", err)
				}
			} else if column.Key() {
				if err := event.AddNewValue(oldValue); err != nil {
					return nil, xerrors.Errorf("Add new value error: %w", err)
				}
			}
		}
		if !keyExists {
			return nil, xerrors.Errorf("Key value for table '%v' not found, maybe PK supplemental logging need to be enabled",
				table.OracleSQLName())
		}
		return event, nil
	default:
		return nil, xerrors.Errorf("Unsupported sql statement type '%v'", opCode)
	}
}

type LogMinerRow struct {
	SCN       uint64    `db:"SCN"`
	Timestamp time.Time `db:"TIMESTAMP"`
	RSID      string    `db:"RS_ID"`
	SSN       uint64    `db:"SSN"`
	XID       string    `db:"XID"`
	OPCode    byte      `db:"OPERATION_CODE"`
	User      *string   `db:"SEG_OWNER"`
	TableName *string   `db:"TABLE_NAME"`
	RowID     string    `db:"ROW_ID"`
	SQLRedo   string    `db:"SQL_REDO"`
	Splitted  byte      `db:"CSF"`
	Status    byte      `db:"STATUS"`
}

func (source *oracleLogMinerSource) pushTransaction(transaction *FinishedTransaction, batch *logMinerBatch, useData bool) error {
	if source.run.startPosition.Type() == oracle_common.PositionSnapshotStarted && transaction.Info.OracleEndPosition().SCN() <= source.run.startPosition.SCN() {
		return nil
	}

	if len(transaction.Rows) > 0 && useData {
		beginTransactionEvent := events.NewDefaultTransactionEvent(transaction.Info.BeginPosition(), transaction.Info, events.TransactionBegin)
		batch.Add(beginTransactionEvent)

		builder := strings.Builder{}
		for _, row := range transaction.Rows {
			// https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-LOGMNR_CONTENTS.html
			// See "CSF" field
			if row.Splitted == 1 {
				builder.WriteString(row.SQLRedo)
				continue
			} else if row.Splitted == 0 && builder.Len() > 0 {
				builder.WriteString(row.SQLRedo)
				row.SQLRedo = builder.String()
				builder.Reset()
			}

			if source.metrics != nil {
				// bytes read from row.SQLRedo
				source.metrics.Size.Add(int64(len(row.SQLRedo)))
			}

			event, err := source.parseSQL(&row, transaction.Info)
			if err != nil {
				source.logger.Error("Parse log miner sql statment error", log.Error(err), log.String("sql", row.SQLRedo))
				return xerrors.Errorf("Parse sql statment error: %w", err)
			}
			if event != nil {
				batch.Add(event)
				if source.metrics != nil {
					source.metrics.ChangeItems.Inc()
					source.metrics.Parsed.Inc()
				}
			}
		}

		if builder.Len() > 0 {
			return xerrors.New("Failed build splitted statement")
		}

		commitTransactionEvent := events.NewDefaultTransactionEvent(transaction.Info.EndPosition(), transaction.Info, events.TransactionBegin)
		batch.Add(commitTransactionEvent)
	}

	batch.SetProgressPosition(transaction.ProgressPosition)

	return nil
}

func (source *oracleLogMinerSource) getLogMinerViewSQL() (string, error) {
	opcodes := []string{
		strconv.Itoa(int(LogMinerOPCodeInsert)),
		strconv.Itoa(int(LogMinerOPCodeDelete)),
		strconv.Itoa(int(LogMinerOPCodeUpdate)),
		strconv.Itoa(int(LogMinerOPCodeCommit)),
		strconv.Itoa(int(LogMinerOPCodeRollback)),
	}

	bannedUsersCondition, err := oracle_common.GetBannedUsersCondition("SEG_OWNER")
	if err != nil {
		//nolint:descriptiveerrors
		return "", err
	}

	bannedTablesCondition, err := oracle_common.GetBannedTablesCondition("TABLE_NAME")
	if err != nil {
		//nolint:descriptiveerrors
		return "", err
	}

	tableIDs := []*oracle_common.TableID{}
	for i := 0; i < source.schemaRepo.SchemasCount(); i++ {
		schema := source.schemaRepo.OracleSchema(i)
		for j := 0; j < schema.TablesCount(); j++ {
			table := schema.OracleTable(j)
			tableIDs = append(tableIDs, table.OracleTableID())
		}
	}
	if len(tableIDs) == 0 {
		return "", xerrors.Errorf("There no tables")
	}
	tablesCondition, err := oracle_common.GetTablesCondition("SEG_OWNER", "TABLE_NAME", tableIDs, true)
	if err != nil {
		//nolint:descriptiveerrors
		return "", err
	}

	sql := fmt.Sprintf(`
select
	SCN, TIMESTAMP, RS_ID, SSN, (XIDUSN||'.'||XIDSLT||'.'||XIDSQN) as XID, OPERATION_CODE, SEG_OWNER, TABLE_NAME, ROW_ID, SQL_REDO, CSF, STATUS
from V$LOGMNR_CONTENTS
where
	OPERATION_CODE in (%v)
	and (SEG_OWNER is null or %v)
	and (TABLE_NAME is null or %v)
	and ((SEG_OWNER is null and TABLE_NAME is null) or %v)`,
		strings.Join(opcodes, ", "), bannedUsersCondition, bannedTablesCondition, tablesCondition)

	return sql, nil
}

// https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-LOGMNR_CONTENTS.html
func (source *oracleLogMinerSource) readLogMinerView(ctx context.Context, connection *sqlx.Conn) error {
	sql, err := source.getLogMinerViewSQL()
	if err != nil {
		return xerrors.Errorf("Can't generate SQL request for LogMiner view from DB: %w", err)
	}

	rows, err := connection.QueryxContext(ctx, sql)
	if err != nil {
		return xerrors.Errorf("Can't get LogMiner view from DB: %w", err)
	}
	defer rows.Close()

	if !source.Running() {
		return nil
	}

	skipOld := !source.run.currentPosition.OnlySCN()
	for rows.Next() {
		if source.run.batchProcessError != nil {
			//nolint:descriptiveerrors
			return source.run.batchProcessError
		}
		if !source.Running() {
			break
		}

		var row LogMinerRow
		if err := rows.StructScan(&row); err != nil {
			return xerrors.Errorf("Can't parse row from DB: %w", err)
		}

		if skipOld {
			if row.RSID == *source.run.currentPosition.RSID() && row.SSN == *source.run.currentPosition.SSN() {
				skipOld = false
			}
			continue
		}

		// TODO: Support for DDL events (OPERATION_CODE field)

		source.run.currentPosition, err = oracle_common.NewLogPosition(row.SCN, &row.RSID, &row.SSN, oracle_common.PositionReplication, row.Timestamp)
		if err != nil {
			return xerrors.Errorf("Can't create position: %w", err)
		}

		source.run.infoMutex.Lock()
		source.run.currentTimestamp = row.Timestamp
		source.run.currentLogReadRowsCount++
		source.run.infoMutex.Unlock()

		if row.Status == 0 {
			switch opCode := LogMinerOPCode(row.OPCode); opCode {
			case LogMinerOPCodeCommit, LogMinerOPCodeRollback:
				if source.run.transactions.ContainsTransaction(row.XID) {
					transaction, err := source.run.transactions.FinishTransaction(row.XID, source.run.currentPosition)
					if err != nil {
						return xerrors.Errorf("Transaction '%v' end error: %w", row.XID, err)
					}
					var useData bool
					switch opCode {
					case LogMinerOPCodeCommit:
						useData = true
					case LogMinerOPCodeRollback:
						useData = false
					default:
						return xerrors.Errorf("Unsupported end transaction operation type '%v'", opCode)
					}
					if err := source.pushTransaction(transaction, source.run.currentBatch, useData); err != nil {
						return xerrors.Errorf("Can't push transaction '%v': %w", row.XID, err)
					}
				}
			case LogMinerOPCodeInsert, LogMinerOPCodeUpdate, LogMinerOPCodeDelete:
				if !source.run.transactions.ContainsTransaction(row.XID) {
					if err := source.run.transactions.StartTransaction(row.XID, source.run.currentPosition); err != nil {
						return xerrors.Errorf("Transaction '%v' begin error: %w", row.XID, err)
					}
				}
				if err := source.run.transactions.AddRowToTransaction(row.XID, &row); err != nil {
					return xerrors.Errorf("Transaction '%v' add row error: %w", row.XID, err)
				}
			default:
				return xerrors.Errorf("Unsupported operation type '%v'", opCode)
			}
		} else {
			source.logger.Warnf("Row bad status: '%v', redo: '%v', position: [%v]", row.Status, row.SQLRedo, source.run.currentPosition)
		}

		if source.run.transactions.Count() == 0 {
			source.run.currentBatch.SetProgressPosition(source.run.currentPosition)
		}

		if source.run.currentBatch.Ready() {
			source.run.batchProcessWait.Wait()
			source.run.batchProcessWait.Add(1)
			go source.pushBatch(source.run.currentBatch)
			source.run.currentBatch = newLogMinerBatch()
		}
	}

	if rows.Err() != nil {
		return xerrors.Errorf("Can't read row from DB: %w", rows.Err())
	}

	source.run.batchProcessWait.Wait()
	if source.run.batchProcessError != nil {
		//nolint:descriptiveerrors
		return source.run.batchProcessError
	}
	if !source.Running() {
		return nil
	}

	source.run.batchProcessWait.Add(1)
	go source.pushBatch(source.run.currentBatch)
	source.run.currentBatch = newLogMinerBatch()
	source.run.batchProcessWait.Wait()
	if source.run.batchProcessError != nil {
		//nolint:descriptiveerrors
		return source.run.batchProcessError
	}

	return nil
}

func (source *oracleLogMinerSource) pushBatch(batch *logMinerBatch) {
	defer source.run.batchProcessWait.Done()

	if !source.Running() {
		return
	}

	if !batch.Empty() {
		if err := <-source.run.target.AsyncPush(abstract2.NewEventBatch(batch.Rows)); err != nil {
			source.run.batchProcessError = xerrors.Errorf("Push events error: %w", err)
			source.logger.Error("Push events error", log.Error(err))
			return
		}
	}

	if !source.Running() {
		return
	}

	if batch.HasProgressPosition() {
		if err := source.tracker.WritePosition(batch.ProgressPosition); err != nil {
			source.run.batchProcessError = xerrors.Errorf("Can't write position to tracker: %w", err)
			source.logger.Error("Can't write position to tracker", log.Error(err))
			return
		}
	}
}

func (source *oracleLogMinerSource) readLogFiles(ctx context.Context, connection *sqlx.Conn) error {
	if err := source.addLogFilesToLogMiner(ctx, connection); err != nil {
		return xerrors.Errorf("Adding log files to LogMiner error: %w", err)
	}

	if err := source.startLogMiner(ctx, connection); err != nil {
		return xerrors.Errorf("Starting LogMiner error: %w", err)
	}

	if err := source.readLogMinerView(ctx, connection); err != nil {
		return xerrors.Errorf("Reading LogMiner view error: %w", err)
	}

	if err := source.stopLogMiner(ctx, connection); err != nil {
		return xerrors.Errorf("Stopping LogMiner error: %w", err)
	}

	return nil
}

func (source *oracleLogMinerSource) setLogFile(ctx context.Context, connection *sqlx.Conn) error {
	logFiles, err := source.getLogFiles(ctx, connection, source.run.currentPosition.SCN())
	if err != nil {
		return xerrors.Errorf("Get log files error: %w", err)
	}
	if len(logFiles) == 0 {
		return xerrors.New("No log files")
	}
	if source.run.currentPosition.SCN() < logFiles[0].FromSCN {
		return xerrors.Errorf("There no suitable log files, position: [%v], nearest log: '%v'", source.run.currentPosition, logFiles[0])
	}

	prevLog := source.run.currentLog

	source.run.infoMutex.Lock()
	defer source.run.infoMutex.Unlock()
	if source.run.currentLog == nil {
		source.run.currentLog = &logFiles[0]
		if source.run.currentPosition.Type() == oracle_common.PositionSnapshotStarted {
			source.run.currentPosition, err = oracle_common.NewLogPosition(source.run.currentLog.FromSCN, nil, nil, oracle_common.PositionReplication, source.run.currentTimestamp)
			if err != nil {
				return xerrors.Errorf("Can't create position: %w", err)
			}
		}
	} else {
		if source.run.endOfTheLog &&
			len(logFiles) > 1 &&
			LogType(source.run.currentLog.Type) == LogTypeArchived &&
			logFiles[0].ID == source.run.currentLog.ID {
			source.run.currentLog = &logFiles[1]
			source.run.currentPosition, err = oracle_common.NewLogPosition(source.run.currentLog.FromSCN, nil, nil, oracle_common.PositionReplication, source.run.currentTimestamp)
			if err != nil {
				return xerrors.Errorf("Can't create position: %w", err)
			}
		} else if logFiles[0].ID == source.run.currentLog.ID {
			source.run.currentLog = &logFiles[0]
		} else {
			if len(logFiles) > 1 {
				return xerrors.Errorf("Log sequence violation, current log: '%v', first candidate: '%v', second candidate: '%v'",
					source.run.currentLog, logFiles[0], logFiles[1])
			} else {
				return xerrors.Errorf("Log sequence violation, current log: '%v', candidate: '%v'", source.run.currentLog, logFiles[0])
			}
		}
	}

	if prevLog == nil {
		source.logger.Infof("Change log, current log: '%v'", source.run.currentLog)
	} else if prevLog.ID != source.run.currentLog.ID {
		source.logger.Infof("Change log, current log: '%v', prev log: '%v'", source.run.currentLog, prevLog)
		if source.run.currentLog.ID-prevLog.ID != 1 {
			source.logger.Warnf("Gap between log files, current log: '%v', prev log: '%v'", source.run.currentLog, prevLog)
		}
	}

	return nil
}

func (source *oracleLogMinerSource) logState() {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			source.run.infoMutex.Lock()

			logFileStr := "nil"
			if source.run.currentLog != nil {
				logFileStr = source.run.currentLog.String()
			}

			positionStr := "nil"
			if source.run.currentPosition != nil {
				positionStr = source.run.currentPosition.String()
			}

			timestampStr := source.run.currentTimestamp.String()

			source.logger.Infof("Current state, log file: '%v', position: [%v], position timestamp: '%v', log read rows count: %v",
				logFileStr, positionStr, timestampStr, humanize.Comma(source.run.currentLogReadRowsCount))

			source.run.currentLogReadRowsCount = 0

			posSCN := uint64(0)
			if source.run.currentPosition != nil {
				posSCN = source.run.currentPosition.SCN()
			}
			source.run.infoMutex.Unlock()

			source.updateUsageMetric(posSCN)
		case <-source.run.ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (source *oracleLogMinerSource) updateUsageMetric(posSCN uint64) {
	if source.metrics == nil || source.run == nil || !source.Running() {
		return
	}
	if posSCN == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = oracle_common.CDBQueryGlobal(source.config, source.sqlxDB, ctx, func(ctx context.Context, connection *sqlx.Conn) error {
		const currentSCNSQL = "SELECT CURRENT_SCN FROM v$database"
		var currentSCN uint64
		if err := connection.GetContext(ctx, &currentSCN, currentSCNSQL); err != nil {
			return xerrors.Errorf("Can't select current SCN from DB: %w", err)
		}

		logFiles, err := source.getLogFiles(ctx, connection, posSCN)
		if err != nil {
			return err
		}
		if len(logFiles) == 0 {
			return nil
		}

		lagBytes := estimateLagBytesForLog(logFiles[0], posSCN, currentSCN)
		if len(logFiles) > 1 && currentSCN > logFiles[0].ToSCN {
			lagBytes += estimateLagBytesForLog(logFiles[1], logFiles[1].FromSCN, currentSCN)
		}

		source.metrics.Usage.Set(float64(lagBytes))
		return nil
	})
}

func estimateLagBytesForLog(log LogFileRow, fromSCN, toSCN uint64) uint64 {
	if log.Bytes == 0 {
		return 0
	}
	if log.ToSCN <= log.FromSCN {
		return 0
	}

	start := fromSCN
	if start < log.FromSCN {
		start = log.FromSCN
	}
	end := toSCN
	if end > log.ToSCN {
		end = log.ToSCN
	}
	if end <= start {
		return 0
	}

	span := log.ToSCN - log.FromSCN
	rem := end - start
	return (log.Bytes * rem) / span
}

func (source *oracleLogMinerSource) Start(ctx context.Context, target abstract2.EventTarget) error {
	if source.run != nil {
		return xerrors.New("Already running")
	}
	startPosition, err := source.tracker.ReadPosition()
	if err != nil {
		return xerrors.Errorf("Can't read position from tracker: %w", err)
	}
	if startPosition == nil {
		return xerrors.New("No position in tracker")
	}

	source.logger.Infof("Start log miner replication, from position: [%v]", startPosition)

	runCtx, cancel := context.WithCancel(ctx)
	//nolint:exhaustivestruct
	source.run = &oracleLogMinerSourceRun{
		ctx:                     runCtx,
		cancel:                  cancel,
		target:                  target,
		currentLog:              nil,
		startPosition:           startPosition,
		currentPosition:         startPosition,
		currentTimestamp:        time.Unix(0, 0),
		currentLogReadRowsCount: 0,
		endOfTheLog:             false,
		transactions:            newTransactionStore(runCtx, source.logger),
		currentBatch:            newLogMinerBatch(),
		batchProcessWait:        sync.WaitGroup{},
		batchProcessError:       nil,
	}
	defer source.Stop()

	go source.logState()

	lastMineStart := time.Now().Add(-minMineTimeout)
	for {
		queryErr := oracle_common.CDBQueryGlobal(source.config, source.sqlxDB, context.Background(),
			func(ctx context.Context, connection *sqlx.Conn) error {
				if err := source.setLogFile(ctx, connection); err != nil {
					return xerrors.Errorf("Set log file error: %w", err)
				}

				source.run.endOfTheLog = false

				if !source.Running() {
					return replicationBreakErr
				}

				sinceLastMine := time.Since(lastMineStart)
				if sinceLastMine < minMineTimeout {
					time.Sleep(minMineTimeout - sinceLastMine)
				}

				lastMineStart = time.Now()
				if err := source.readLogFiles(ctx, connection); err != nil {
					if stopErr := source.stopLogMiner(ctx, connection); stopErr != nil {
						source.logger.Warn("Error stopping LogMiner after reading log error", log.Error(stopErr))
					}
					if IsContainsError(err, skipErrorCodes) {
						source.logger.Warn("Skip reading log error", log.Error(err))
						return replicationContinueErr
					}
					return xerrors.Errorf("Read log file '%v' error: %w", source.run.currentLog, err)
				} else {
					source.run.endOfTheLog = true
				}

				return nil
			})

		if queryErr != nil {
			if queryErr == replicationContinueErr {
				continue
			}
			if queryErr == replicationBreakErr {
				break
			}
			return queryErr
		}

		if !source.Running() {
			break
		}

	}

	return nil
}

func (source *oracleLogMinerSource) Stop() error {
	if source.run != nil {
		source.run.cancel()
	}
	return nil
}
