package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var createTableTemplate, _ = template.New("query").Parse(`
{{- /*gotype: TemplateModel*/ -}}
CREATE TABLE IF NOT EXISTS {{ .Table }} (
{{ range .Cols }}{{ .Name }} {{ .Typ }} {{ .Comma }}
{{ end }} {{ if .Keys }} PRIMARY KEY ({{ range .Keys }}{{ .Name }}{{ .Comma }}{{ end }}) {{ end }}
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
`)

const estimateTableRowsCount = `
SELECT count(*)
FROM information_schema.TABLES
WHERE (TABLE_SCHEMA = ?) AND (TABLE_NAME = ?)
`

const (
	ErrCodeDuplicateKey = 1062
	ErrCodeSyntax       = 1064
	ErrCodeLockTimeout  = 1205
	ErrCodeDeadlock     = 1213
)

const maxSampleLen = 10000

type txBatch struct {
	gtid           string
	queries        []string
	size           int
	tableRowCounts map[abstract.TableID]int
	lsn            uint64
}

type TemplateCol struct {
	Name, Typ, Comma string
}
type TemplateModel struct {
	Cols  []TemplateCol
	Keys  []TemplateCol
	Table string
}

func generatedBounds(queries []string, maxSize int) []abstract.TxBound {
	size := 0
	left := 0
	var bounds []abstract.TxBound
	for i, q := range queries {
		if size+len(q) > maxSize {
			bounds = append(bounds, abstract.TxBound{
				Left:  left,
				Right: i + 1,
			})
			left = i + 1
			size = 0
		} else {
			size = size + len(q)
		}
	}
	if len(queries) != left {
		bounds = append(bounds, abstract.TxBound{
			Left:  left,
			Right: len(queries),
		})
	}
	return bounds
}

func (t *txBatch) txQueries() []string {
	var res []string
	bounds := generatedBounds(t.queries, 1024*1024)
	for _, b := range bounds {
		res = append(res, strings.Join(t.queries[b.Left:b.Right], "\n"))
	}
	return res
}

type sinker struct {
	cache              abstract.DBSchema
	db                 *sql.DB
	metrics            *stats.SinkerStats
	config             *MysqlDestination
	logger             log.Logger
	limit              int
	uniqConstraints    map[string]bool
	progress           LsnProgress
	progressState      map[string]TableStatus
	rw                 sync.Mutex
	currentTX          *sql.Tx
	currentTXID        string
	pendingTableCounts map[abstract.TableID]int
}

func (s *sinker) fillUniqueConstraints() error {
	newUniqConstraints, err := fillUniqConstraints(s.db)
	if err != nil {
		return xerrors.Errorf("Cannot get unique constraints from the target database: %w", err)
	}
	s.uniqConstraints = newUniqConstraints
	return nil
}

func (s *sinker) disableParallelWrite(table string) (bool, error) {
	if s.config.DisableParallelWrite[table] {
		s.logger.Info("Parallel write disabled due to configuration", log.String("table", table))
		return true, nil
	}
	if s.uniqConstraints == nil {
		err := s.fillUniqueConstraints()
		if err != nil {
			return false, err
		}
	}
	if s.uniqConstraints[table] {
		s.logger.Info("Parallel write disabled due to unique constraint", log.String("table", table))
		return true, nil
	}
	return false, nil
}

func (s *sinker) Push(input []abstract.ChangeItem) error {
	if s.config.PerTransactionPush {
		if err := s.perTransactionPush(input); err != nil {
			return xerrors.Errorf("unable to execute per transaction push: %w", err)
		}
	} else {
		tables, err := s.prepareInputPerTables(input)
		if err != nil {
			return xerrors.Errorf("unable to prepare per table input: %w", err)
		}
		if err := s.perTablePush(tables); err != nil {
			return xerrors.Errorf("unable to execute per table push: %w", err)
		}
	}
	return nil
}

func (s *sinker) prepareInputPerTables(input []abstract.ChangeItem) (map[abstract.TableID][]abstract.ChangeItem, error) {
	tables := make(map[abstract.TableID][]abstract.ChangeItem)
	for _, row := range input {
		mustSetDB := false
		db := s.config.Database
		if db == "" {
			mustSetDB = true
			db = row.Schema
		}
		tableID := abstract.TableID{
			Namespace: db,
			Name:      row.Table,
		}
		switch row.Kind {
		case abstract.DDLKind:
			statement := row.ColumnValues[0].(string)
			ddlQ := ""
			if s.config.SQLMode != "default" {
				ddlQ += fmt.Sprintf("SET SESSION sql_mode='%v';\n", s.config.SQLMode)
			}
			ddlQ += DisableFKQuery
			if !emptyTableID(row.TableID()) {
				ddlQ += setFqtn(statement, row.TableID(), tableID, mustSetDB)
			} else {
				ddlQ += statement
			}
			if _, err := s.db.Exec(ddlQ); err != nil {
				s.logger.Warn("Unable to exec DDL:\n"+util.Sample(ddlQ, maxSampleLen), log.Error(err))
				if IsErrorCode(err, ErrCodeSyntax) {
					return nil, abstract.NewFatalError(coded.Errorf(CodeSyntax, "%w", err))
				}
				return nil, xerrors.Errorf("unable to execute ddl: %w", err)
			} else {
				s.logger.Infof("Done DDL:\n%v", util.Sample(ddlQ, maxSampleLen))
			}
			err := s.fillUniqueConstraints()
			if err != nil {
				return nil, xerrors.Errorf("unable to fill unique constraints: %w", err)
			}
		case abstract.DropTableKind:
			if s.config.Cleanup != model.Drop {
				s.logger.Infof("Skipped dropping table '%v.%v' due cleanup policy", db, row.Table)
				continue
			}
			ddlQ := EnableFKQuery
			if s.config.SkipKeyChecks {
				ddlQ = DisableFKQuery
			}
			ddlQ += fmt.Sprintf("DROP TABLE IF EXISTS `%v`.`%v`", db, row.Table)
			if _, err := s.db.Exec(ddlQ); err != nil {
				s.logger.Warn("Unable to exec DDL:\n"+util.Sample(ddlQ, maxSampleLen), log.Error(err))
				return nil, xerrors.Errorf("unable to drop table: %w", err)
			} else {
				s.logger.Infof("Done DDL:\n%v", util.Sample(ddlQ, maxSampleLen))
			}
		case abstract.TruncateTableKind:
			if s.config.Cleanup != model.Truncate {
				s.logger.Infof("Skipped truncating table '%v.%v' due cleanup policy", db, row.Table)
				continue
			}
			var cnt int
			err := s.db.QueryRow(estimateTableRowsCount, db, row.Table).Scan(&cnt)
			if err != nil {
				return nil, xerrors.Errorf("unable to select table rows count: %w", err)
			}
			if cnt == 0 {
				s.logger.Infof("table `%v`.`%v` not exist, continue", db, row.Table)
				continue
			}
			ddlQ := DisableFKQuery
			ddlQ += fmt.Sprintf("TRUNCATE TABLE `%v`.`%v`", db, row.Table)
			if _, err := s.db.Exec(ddlQ); err != nil {
				s.logger.Warn("Unable to exec DDL:\n"+util.Sample(ddlQ, maxSampleLen), log.Error(err))
				return nil, xerrors.Errorf("unable to truncate table: %w", err)
			} else {
				s.logger.Infof("Done DDL:\n%v", util.Sample(ddlQ, maxSampleLen))
			}
		case abstract.InitShardedTableLoad, abstract.SynchronizeKind:
			// not needed for now
		case abstract.InitTableLoad:
			initQ := s.progress.BuildLSNQuery(tableID.Fqtn(), row.LSN, SnapshotWait)
			s.logger.Infof("init table load: %v", initQ)
			if _, err := s.db.Exec(initQ); err != nil {
				return nil, xerrors.Errorf("unable to execute lsn query: %w", err)
			}
			if err := s.refreshState(); err != nil {
				return nil, xerrors.Errorf("unable to refresh target state: %w", err)
			}
		case abstract.DoneTableLoad:
			doneQ := s.progress.BuildLSNQuery(tableID.Fqtn(), row.LSN, SyncWait)
			s.logger.Infof("done table load: %v", doneQ)
			if _, err := s.db.Exec(doneQ); err != nil {
				return nil, xerrors.Errorf("unable to execute lsn query: %w", err)
			}
			if err := s.refreshState(); err != nil {
				return nil, xerrors.Errorf("unable to refresh target state: %w", err)
			}
		case abstract.DoneShardedTableLoad:
			// not needed for now
		case abstract.InsertKind, abstract.UpdateKind, abstract.DeleteKind:
			if len(tables[tableID]) == 0 {
				tables[tableID] = make([]abstract.ChangeItem, 0)
			}
			tables[tableID] = append(tables[tableID], row)
		default:
			s.logger.Infof("kind: %v not supported", row.Kind)
		}
	}
	for tableID, rows := range tables {
		if err := s.checkTable(tableID, rows[0].TableSchema); err != nil {
			return nil, err
		}
	}
	return tables, nil
}

func (s *sinker) refreshState() error {
	s.rw.Lock()
	defer s.rw.Unlock()
	state, err := s.progress.GetCurrentState()
	if err != nil {
		return err
	}
	s.progressState = state
	return nil
}

func emptyTableID(id abstract.TableID) bool {
	return id.Namespace == "" && id.Name == ""
}

func (s *sinker) perTransactionPush(input []abstract.ChangeItem) error {
	s.logger.Infof("Prepare batch %v", len(input))
	var txs []txBatch
	for _, tx := range abstract.SplitByID(input) {
		batch := input[tx.Left:tx.Right]
		size := 0
		tableRowCounts := map[abstract.TableID]int{}
		var queries []string
		for i, row := range batch {
			db := s.config.Database
			mustSetDB := false
			if db == "" {
				mustSetDB = true
				db = row.Schema
			}
			tableID := abstract.TableID{
				Namespace: db,
				Name:      row.Table,
			}
			switch row.Kind {
			case abstract.InsertKind, abstract.UpdateKind, abstract.DeleteKind:
				batch, err := s.buildQueries(tableID, row.TableSchema.Columns(), batch[i:i+1])
				if err != nil {
					return xerrors.Errorf("Can't build queries for table '%v' batch: %w", tableID.Fqtn(), err)
				}
				for _, query := range batch {
					queries = append(queries, query.query)
				}
				tableRowCounts[tableID]++
			case abstract.DDLKind:
				statement := row.ColumnValues[0].(string)
				ddlQ := ""
				if s.config.SQLMode != "default" {
					ddlQ += fmt.Sprintf("SET SESSION sql_mode='%v';\n", s.config.SQLMode)
				}
				ddlQ += DisableFKQuery
				if !emptyTableID(row.TableID()) {
					ddlQ += setFqtn(statement, row.TableID(), tableID, mustSetDB)
				} else {
					ddlQ += statement
				}
				queries = append(queries, ddlQ)
			case abstract.DropTableKind:
				if s.config.Cleanup != model.Drop {
					s.logger.Infof("Skipped dropping table '%v.%v' due cleanup policy", db, row.Table)
					continue
				}
				ddlQ := DisableFKQuery
				ddlQ += fmt.Sprintf("DROP TABLE IF EXISTS `%v`.`%v`", db, row.Table)
				if _, err := s.db.Exec(ddlQ); err != nil {
					s.logger.Warn("Unable to exec DDL:\n"+util.Sample(ddlQ, maxSampleLen), log.Error(err))
					return xerrors.Errorf("unable to drop table: %w", err)
				} else {
					s.logger.Infof("Done DDL:\n%v", util.Sample(ddlQ, maxSampleLen))
				}
			case abstract.TruncateTableKind:
				if s.config.Cleanup != model.Truncate {
					s.logger.Infof("Skipped truncating table '%v.%v' due cleanup policy", db, row.Table)
					continue
				}
				var cnt int
				err := s.db.QueryRow(estimateTableRowsCount, db, row.Table).Scan(&cnt)
				if err != nil {
					return xerrors.Errorf("unable to select table rows count: %w", err)
				}
				if cnt == 0 {
					s.logger.Infof("table `%v`.`%v` not exist, continue", db, row.Table)
					continue
				}
				ddlQ := DisableFKQuery
				ddlQ += fmt.Sprintf("TRUNCATE TABLE `%v`.`%v`", db, row.Table)
				queries = append(queries, ddlQ)
			case abstract.InitShardedTableLoad, abstract.DoneShardedTableLoad, abstract.SynchronizeKind:
				// not needed for now
			case abstract.InitTableLoad:
				initQ := s.progress.BuildLSNQuery(tableID.Fqtn(), row.LSN, SnapshotWait)
				s.logger.Infof("init table load: %v", initQ)
				queries = append(queries, initQ)
			case abstract.DoneTableLoad:
				doneQ := s.progress.BuildLSNQuery(tableID.Fqtn(), row.LSN, SyncWait)
				s.logger.Infof("done table load: %v", doneQ)
				queries = append(queries, doneQ)
			default:
				return xerrors.Errorf("not implemented change kind: %v", row.Kind)
			}
		}
		for _, q := range queries {
			size += len(q)
		}
		if size > 0 {
			txs = append(txs, txBatch{
				gtid:           batch[0].TxID,
				queries:        queries,
				size:           size,
				tableRowCounts: tableRowCounts,
				lsn:            batch[len(batch)-1].LSN,
			})
		}
	}
	if len(txs) == 0 {
		return nil
	}
	if len(txs) < 3 {
		// we have 1 or 2 transaction, no need to squash in between transactions
		for _, tx := range txs {
			if err := s.runTX(tx); err != nil {
				return err
			}
		}
	} else {
		// we have at least 3 tx, there for
		// every tx except last one can be squashed into one large tx
		// last tx will just closed squashed in between txs and open new one, but not committed it
		var inBetweenTX txBatch
		inBetweenTX.queries = []string{}
		inBetweenTX.tableRowCounts = map[abstract.TableID]int{}
		var squashedTXs []string
		for _, tx := range txs[0 : len(txs)-1] {
			squashedTXs = append(squashedTXs, tx.gtid)
			inBetweenTX.queries = append(inBetweenTX.queries, tx.queries...)
			inBetweenTX.gtid = tx.gtid
			inBetweenTX.lsn = tx.lsn
			for table, count := range tx.tableRowCounts {
				inBetweenTX.tableRowCounts[table] += count
			}
		}
		s.logger.Infof("squashed %v into %v", append([]string{s.currentTXID}, squashedTXs...), inBetweenTX.gtid)
		// run squashed in between txs
		if err := s.runTX(inBetweenTX); err != nil {
			return xerrors.Errorf("unable to run in-between batch transaction: %w", err)
		}

		// begin last batch tx
		if err := s.runTX(txs[len(txs)-1]); err != nil {
			return xerrors.Errorf("unable to run last batch transaction: %w", err)
		}
	}
	return nil
}

func (s *sinker) runTX(batch txBatch) error {
	if s.currentTXID != batch.gtid && s.currentTX != nil {
		var lsnProgressQueries []string
		for t := range s.pendingTableCounts {
			lsnProgressQueries = append(lsnProgressQueries, s.progress.BuildLSNQuery(t.Fqtn(), batch.lsn-1, InSync))
		}
		if len(lsnProgressQueries) > 0 {
			s.logger.Infof("store progress: %v", lsnProgressQueries)
			if _, err := s.currentTX.Exec(strings.Join(lsnProgressQueries, "\n")); err != nil {
				return xerrors.Errorf("unable to exec progress: %w, query: %v", err, util.Sample(strings.Join(lsnProgressQueries, ";"), 500))
			}
		}
		if err := s.currentTX.Commit(); err != nil {
			return xerrors.Errorf("unable to commit transaction %v: %w", s.currentTXID, err)
		}
		oldTXID := s.currentTXID
		s.currentTX = nil
		var txRes []string
		for t, c := range s.pendingTableCounts {
			s.metrics.Table(t.Fqtn(), "rows", c)
			s.metrics.Table(t.Name, "rows", c) // for backward compatibility
			s.progressState[t.Fqtn()] = TableStatus{
				LSN:    batch.lsn - 1,
				Status: InSync,
			}
			txRes = append(txRes, fmt.Sprintf("%v.%v(%v rows)", t.Namespace, t.Name, c))
		}
		s.logger.Infof("committed tx: %v, table set: %v, next gtid: %v", oldTXID, strings.Join(txRes, ","), batch.gtid)
		s.pendingTableCounts = map[abstract.TableID]int{}
	}
	var err error
	if s.currentTX == nil {
		s.currentTX, err = s.db.Begin()
		if err != nil {
			return xerrors.Errorf("unable to begin transaction: %w", err)
		}
		s.currentTXID = batch.gtid

		if _, err := s.currentTX.Exec(s.queryHeader().query); err != nil {
			return xerrors.Errorf("unable to exec tx set modes: %w", err)
		}
	}
	for _, txQuery := range batch.txQueries() {
		s.logger.Debugf("execute tx%v query: \n%v", batch.gtid, util.Sample(txQuery, 100))
		_, err = s.currentTX.Exec(txQuery)
		if err != nil {
			return xerrors.Errorf("unable exec query for tx: %v: %w", batch.gtid, err)
		}
	}
	for t, rowCount := range batch.tableRowCounts {
		s.pendingTableCounts[t] += rowCount
	}
	return nil
}

func (s *sinker) perTablePush(tables map[abstract.TableID][]abstract.ChangeItem) error {
	start := time.Now()
	lastLSNs := map[string]uint64{}
	errCh := make(chan error, len(tables))
	wg := sync.WaitGroup{}
	// will split traffic per table transactions
	for tableID, rows := range tables {
		wg.Add(1)
		if err := s.checkTable(tableID, rows[0].TableSchema); err != nil {
			return xerrors.Errorf("checking table %s failed: %w", tableID.Fqtn(), err)
		}
	}
	for table, rows := range tables {
		includeIDX := 0
		excludeIDX := -1
		lastLSN := rows[len(rows)-1].LSN
		for i, r := range rows {
			// Disable for public cloud for now
			if r.LSN < s.progressState[table.Fqtn()].LSN && s.progressState[table.Fqtn()].Status == SyncWait && r.LSN != 0 {
				excludeIDX = i
			} else {
				break
			}
		}
		if excludeIDX == len(rows)-1 {
			s.logger.Infof(
				"table skipped completely: %v (%v rows), status: %v, LSN: %v > %v",
				table,
				len(rows),
				s.progressState[table.Fqtn()].Status,
				s.progressState[table.Fqtn()].LSN,
				lastLSN,
			)
			wg.Done()
			continue
		}
		if excludeIDX >= 0 {
			// Whole batch need to be inserted
			includeIDX = excludeIDX + 1
		}
		go func(table abstract.TableID, rows []abstract.ChangeItem) {
			defer wg.Done()
			disableParallelWrite, err := s.disableParallelWrite(table.Fqtn())
			if err != nil {
				s.logger.Warn("Cannot check unique constraints on table %s", log.Error(err))
				errCh <- xerrors.Errorf("Cannot check unique constraints on table %s: %w", table.Fqtn(), err)
				return
			}
			queries := []sinkQuery{s.queryHeader()}
			dataQueries, err := s.buildQueries(table, rows[0].TableSchema.Columns(), rows)
			if err != nil {
				errCh <- xerrors.Errorf("Can't build queries for table '%v' batch: %w", table.Fqtn(), err)
				return
			}
			queries = append(queries, dataQueries...)
			if s.progressState[table.Fqtn()].Status != SnapshotWait {
				lsnQuery := *newSinkQuery(s.progress.BuildLSNQuery(table.Fqtn(), lastLSN, InSync), false)
				queries = append(queries, lsnQuery)
			}
			if disableParallelWrite || len(rows) < 1000 {
				// if less then 1k rows, no need to split in tx-s
				// if has uniq constraint we must do everything in one tx to prevent uniq contraint fails; see TM-1284
				if err := s.txPush(table, queries); err != nil {
					s.logger.Warn("Unable to perform queries sequentially", log.Error(err))
					errCh <- err
					return
				}
			} else {
				if err := s.txParallel(table, queries); err != nil {
					s.logger.Warn("Unable to perform queries in parallel", log.Error(err))
					errCh <- err
					return
				}
			}
			if err := s.refreshState(); err != nil {
				errCh <- err
				return
			}
			s.metrics.Table(table.Fqtn(), "rows", len(rows))
			s.metrics.Table(table.Name, "rows", len(rows)) // for backward compatibility
		}(table, rows[includeIDX:])
	}
	wg.Wait()
	close(errCh)
	var errR error
	for err := range errCh {
		if err != nil {
			s.logger.Error("push error", log.Error(err))
			errR = xerrors.Errorf("error: %w", err)
		}
	}
	if errR == nil {
		for k, lsn := range lastLSNs {
			if s.progressState[k].Status != SnapshotWait {
				s.progressState[k] = TableStatus{
					LSN:    lsn,
					Status: InSync,
				}
			}
		}
	}
	s.logger.Infof("done push in %v", time.Since(start))
	s.metrics.Elapsed.RecordDuration(time.Since(start))
	return errR
}

const parallelism = 4

func (s *sinker) txParallel(table abstract.TableID, queries []sinkQuery) error {
	if len(queries) == 0 {
		return nil
	}
	iCh := make(chan error, len(queries))

	wg := sync.WaitGroup{}
	semaphoreCh := make(chan struct{}, parallelism)
	sequentialQueries := []sinkQuery{s.queryHeader()}
	for _, query := range queries {
		if !query.parallel {
			sequentialQueries = append(sequentialQueries, query)
			continue
		}

		// > 1 because sequentialQueries init with s.queryHeader()
		if len(sequentialQueries) > 1 {
			wg.Wait()
			iCh <- s.txPush(table, sequentialQueries)
			sequentialQueries = []sinkQuery{s.queryHeader()}
		}

		wg.Add(1)
		semaphoreCh <- struct{}{}
		go func(query sinkQuery) {
			defer wg.Done()
			iCh <- s.txPush(table, []sinkQuery{s.queryHeader(), query})
			<-semaphoreCh
		}(query)
	}
	wg.Wait()

	// > 1 because sequentialQueries init with s.queryHeader()
	if len(sequentialQueries) > 1 {
		iCh <- s.txPush(table, sequentialQueries)
	}

	close(iCh)

	var errRes error
	for err := range iCh {
		if err != nil {
			if errRes != nil {
				errRes = xerrors.Errorf("%v\n%w", errRes, err)
			} else {
				errRes = err
			}
		}
	}
	return errRes
}

func (s *sinker) queryHeader() sinkQuery {
	var args string
	if s.config.SkipKeyChecks {
		args += DisableFKQuery
		args += "SET UNIQUE_CHECKS=0;\n"
	} else {
		args += EnableFKQuery
		args += "SET UNIQUE_CHECKS=1;\n"
	}
	if s.config.SQLMode != "default" {
		args += fmt.Sprintf("SET SESSION sql_mode='%v';\n", s.config.SQLMode)
	}
	return *newSinkQuery(args, false)
}

func (s *sinker) txPushImpl(table abstract.TableID, queries []sinkQuery) error {
	totalQ := 0
	st := time.Now()
	for _, q := range queries {
		totalQ += len(q.query)
		s.metrics.Table(table.Fqtn(), "size", len(q.query))
		s.metrics.Table(table.Name, "size", len(q.query)) // for backward compatibility
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*15)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("unable to begin transaction: %w", err)
	}
	stP := time.Now()
	if err := s.pushQuires(tx, queries); err != nil {
		return err
	}
	stC := time.Now()
	if err := tx.Commit(); err != nil {
		msg := "commit transaction error"
		s.logger.Error(msg, log.Error(err))
		return xerrors.Errorf("%v: %w", msg, err)
	}
	s.logger.Infof(
		"table [%v] pushed %v queries (%v size) in %v (%v push %v commit)",
		table.Fqtn(),
		len(queries),
		format.SizeInt(totalQ),
		time.Since(st),
		time.Since(stP),
		time.Since(stC),
	)
	return nil
}

func wrapErrorIfFatal(err error) error {
	if err == mysql.ErrPktTooLarge {
		return abstract.NewFatalError(err)
	} else {
		return err
	}
}

func (s *sinker) txPush(table abstract.TableID, queries []sinkQuery) error {
	err := s.txPushImpl(table, queries)
	return wrapErrorIfFatal(err)
}

func (s *sinker) pushQuires(tx *sql.Tx, queries []sinkQuery) error {
	for _, q := range queries {
		start := time.Now()
		if _, err := tx.Exec(q.query); err != nil {
			if IsErrorCode(err, ErrCodeDeadlock) {
				return coded.Errorf(CodeDeadlock, "a deadlock occurred while executing query: %w", err)
			}
			s.logger.Error(fmt.Sprintf("exec error, query:\n%v", util.Sample(q.query, maxSampleLen)), log.Duration("elapsed", time.Since(start)), log.Error(err))
			if tErr := tx.Rollback(); tErr != nil {
				return xerrors.Errorf("err: %w\nrollback err: %v", err, tErr)
			}
			return xerrors.Errorf("unable to execute query: %w", err)
		}
	}
	return nil
}

func (s *sinker) Close() error {
	if s.currentTX != nil {
		if err := s.currentTX.Rollback(); err != nil {
			s.logger.Warn("Failed to rollback the current transaction", log.Error(err))
		}
	}
	if err := s.db.Close(); err != nil {
		return xerrors.Errorf("failed to close db: %w", err)
	}
	return nil
}

func (s *sinker) checkTable(tableID abstract.TableID, tableSchema *abstract.TableSchema) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	// MaintainTables historically was used incorrectly. Have to keep it for backward compatibility until we move to
	// GetIsSchemaMigrationDisabled flag completely
	if s.config.IsSchemaMigrationDisabled || s.config.MaintainTables {
		return nil
	}
	if err := s.ensureTableSchema(tableID, tableSchema); err != nil {
		return xerrors.Errorf("failed to ensure table schema: %w", err)
	}
	if s.cache[tableID].Equal(tableSchema) {
		return nil
	}
	if err := s.createTable(tableID, tableSchema); err != nil {
		return xerrors.Errorf("failed to create table: %w", err)
	}

	if err := s.alterTable(tableID, tableSchema); err != nil {
		return xerrors.Errorf("failed to alter table: %w", err)
	}
	return nil
}

func (s *sinker) ensureTableSchema(tableID abstract.TableID, input *abstract.TableSchema) error {
	if schema, ok := s.cache[tableID]; ok && schema.Equal(input) {
		return nil
	}
	tables, err := LoadSchema(s.db, false, false)
	if err != nil {
		return xerrors.Errorf("failed to load schema: %w", err)
	}
	s.cache = tables
	return nil
}

func (s *sinker) createTable(tableID abstract.TableID, ts *abstract.TableSchema) error {
	if _, ok := s.cache[tableID]; ok {
		return nil
	}
	query := prepareCreateTableQuery(tableID, ts)
	if len(query) == 0 {
		return nil
	}
	if _, err := s.db.Exec(query); err != nil {
		s.logger.Warnf("Unable to push DDL (%v):\n%v\n", err, util.Sample(query, maxSampleLen))
		return abstract.NewFatalError(xerrors.Errorf("unable to execute ddl: %w", err))
	}
	s.cache[tableID] = ts
	return nil
}

func (s *sinker) alterTable(tableID abstract.TableID, in *abstract.TableSchema) error {
	curr := s.cache[tableID]
	if curr.Equal(in) {
		return nil
	}
	query := prepareAlterTableQuery(tableID, curr, in)
	if len(query) == 0 {
		return nil
	}
	if _, err := s.db.Exec(query); err != nil {
		s.logger.Warnf("Unable to alter %s (%s): %s", tableID.Fqtn(), util.Sample(query, maxSampleLen), err)
		return abstract.NewFatalError(xerrors.Errorf("unable to alter table %s: %w", tableID.Fqtn(), err))
	}
	return nil
}

func fillUniqConstraints(db *sql.DB) (map[string]bool, error) {
	rows, err := db.Query(`
select TABLE_SCHEMA, TABLE_NAME, count(*)
from INFORMATION_SCHEMA.STATISTICS
where INDEX_NAME != 'PRIMARY'
group by TABLE_SCHEMA, TABLE_NAME;
`)
	if err != nil {
		return nil, xerrors.Errorf("unable to select table statistics: %w", err)
	}
	defer rows.Close()
	constraint := map[string]bool{}
	for rows.Next() {
		var schema, table string
		var count int
		if err := rows.Scan(&schema, &table, &count); err != nil {
			return nil, err
		}
		constraint[fmt.Sprintf("`%v`.`%v`", schema, table)] = true
		constraint[fmt.Sprintf("%v.\"%v\"", schema, table)] = true
		constraint[fmt.Sprintf("\"%v\".\"%v\"", schema, table)] = true
		constraint[fmt.Sprintf("%v.%v", schema, table)] = true
		constraint[fmt.Sprintf("`%v`", table)] = true
		constraint[fmt.Sprintf("\"%v\"", table)] = true
		constraint[fmt.Sprintf("%v", table)] = true
	}
	if rows.Err() != nil {
		return nil, xerrors.Errorf("unable to read table statistics rows: %w", err)
	}
	return constraint, nil
}

func NewSinker(lgr log.Logger, cfg *MysqlDestination, mtrcs metrics.Registry) (abstract.Sinker, error) {
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()

	connectionParams, err := NewConnectionParams(cfg.ToStorageParams())
	if err != nil {
		return nil, xerrors.Errorf("Can't create connection params: %w", err)
	}

	configAction := func(config *mysql.Config) error {
		config.MultiStatements = true
		config.MaxAllowedPacket = 0
		return nil
	}
	db, err := Connect(connectionParams, configAction)
	if err != nil {
		return nil, xerrors.Errorf("Can't connect to server: %w", err)
	}
	rollbacks.AddCloser(db, logger.Log, "cannot close database")

	var n string
	var limit int
	if err := db.QueryRow("SHOW VARIABLES LIKE 'max_allowed_packet';").Scan(&n, &limit); err != nil {
		return nil, err
	}
	lgr.Infof("max allowed packet infered: %v", format.SizeInt(limit))
	progress, err := NewTableProgressTracker(db, cfg.ProgressTrackerDB)
	if err != nil {
		lgr.Warn("Unable to init progress tracker")
		return nil, xerrors.Errorf("Can`t init progress tracker: %w", err)
	}
	progressState, err := progress.GetCurrentState()
	if err != nil {
		return nil, xerrors.Errorf("Can`t get current progress state: %w", err)
	}
	if len(progressState) > 0 {
		lgr.Info("progress state", log.Any("progress", progressState))
	}

	rollbacks.Cancel()
	return &sinker{
		cache:              make(map[abstract.TableID]*abstract.TableSchema),
		db:                 db,
		metrics:            stats.NewSinkerStats(mtrcs),
		config:             cfg,
		logger:             lgr,
		limit:              limit,
		uniqConstraints:    map[string]bool{},
		progressState:      progressState,
		progress:           progress,
		rw:                 sync.Mutex{},
		currentTX:          nil,
		currentTXID:        "",
		pendingTableCounts: map[abstract.TableID]int{},
	}, nil
}
