//go:build !disable_mysql_provider

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	mysql_driver "github.com/go-sql-driver/mysql"
	"github.com/pingcap/parser"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var globalExclude = []*regexp.Regexp{
	regexp.MustCompile(`^mysql\..*`),
	regexp.MustCompile(`^performance_schema\..*`),
	regexp.MustCompile(`^heartbeat\..*`),
}

// Canal can sync your MySQL data into everywhere, like Elasticsearch, Redis, etc...
// MySQL must open row format for binlog
type Canal struct {
	cfg *Config

	parser *parser.Parser
	master *masterInfo
	syncer *replication.BinlogSyncer
	logger log.Logger

	eventHandler EventHandler

	connLock sync.Mutex
	conn     *client.Conn

	tableLock          sync.RWMutex
	tables             map[string]*schema.Table
	errorTablesGetTime map[string]time.Time

	tableMatchCache map[string]bool

	delay *uint32

	ctx    context.Context
	cancel context.CancelFunc
}

// canal will retry fetching unknown table's meta after UnknownTableRetryPeriod
var (
	UnknownTableRetryPeriod = time.Second * time.Duration(10)
	ErrExcludedTable        = xerrors.New("table is excluded")
)

func NewCanal(cfg *Config) (*Canal, error) {
	var rollbacks util.Rollbacks
	defer rollbacks.Do()

	c := new(Canal)
	c.cfg = cfg

	c.ctx, c.cancel = context.WithCancel(context.Background())
	rollbacks.Add(c.cancel)

	c.eventHandler = &DummyEventHandler{}
	c.parser = parser.New()
	c.tables = make(map[string]*schema.Table)
	if c.cfg.DiscardNoMetaRowEvent {
		c.errorTablesGetTime = make(map[string]time.Time)
	}
	c.master = new(masterInfo)
	c.logger = logger.Log
	c.delay = new(uint32)
	rollbacks.Add(c.Close)

	var err error

	if err = c.prepareSyncer(); err != nil {
		return nil, xerrors.Errorf("failed to prepare syncer: %w", err)
	}

	if err := c.checkBinlogRowFormat(); err != nil {
		return nil, xerrors.Errorf("failed to check bin log row format: %w", err)
	}
	if c.cfg.Include == nil {
		c.cfg.Include = func(db string, table string) bool {
			return true
		}
	}
	c.tableMatchCache = make(map[string]bool)
	rollbacks.Cancel()
	return c, nil
}

func (c *Canal) GetDelay() uint32 {
	return atomic.LoadUint32(c.delay)
}

// Run sync from the binlog position in the data.
// It will run forever until meeting an error or Canal closed.
func (c *Canal) Run() error {
	return c.run()
}

// RunFrom will sync from the binlog position directly, ignore mysqldump.
func (c *Canal) RunFrom(pos mysql.Position) error {
	c.master.Update(pos)

	return c.Run()
}

func (c *Canal) StartFromGTID(set mysql.GTIDSet) error {
	c.master.UpdateGTIDSet(set)

	return c.Run()
}

func (c *Canal) run() error {
	defer func() {
		c.cancel()
	}()

	c.master.UpdateTimestamp(uint32(time.Now().Unix()))
	if err := c.runSyncBinlog(); err != nil {
		if !xerrors.Is(err, context.Canceled) {
			c.logger.Errorf("failed to start binlog sync: %v", err)
			return xerrors.Errorf("failed to start binlog sync: %w", err)
		}
	}

	return nil
}

func (c *Canal) Close() {
	c.logger.Infof("closing canal")

	c.cancel()
	if c.syncer != nil {
		c.syncer.Close()
		c.syncer = nil
	}

	c.connLock.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.connLock.Unlock()

	_ = c.eventHandler.OnPosSynced(c.master.Position(), c.master.GTIDSet(), true)
}

func (c *Canal) Ctx() context.Context {
	return c.ctx
}

func (c *Canal) checkTableMatch(db, table string) bool {
	key := fmt.Sprintf("%v.%v", db, table)
	// no filter, return true
	if c.tableMatchCache == nil {
		return true
	}

	c.tableLock.RLock()
	rst, ok := c.tableMatchCache[key]
	c.tableLock.RUnlock()
	if ok {
		// cache hit
		return rst
	}
	matchFlag := c.cfg.Include(db, table)
	if matchFlag && c.globalExcluded(key) {
		matchFlag = false
	}
	c.tableLock.Lock()
	c.tableMatchCache[key] = matchFlag
	c.tableLock.Unlock()
	return matchFlag
}

func (c *Canal) globalExcluded(key string) bool {
	for _, r := range globalExclude {
		if r.MatchString(key) {
			return true
		}
	}
	return false
}

func findDecimalColumn(table *schema.Table) int {
	for i, column := range table.Columns {
		if strings.HasPrefix(strings.ToLower(column.RawType), "decimal") {
			return i
		}
	}
	return -1
}

func (c *Canal) GetTable(db string, table string) (*schema.Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	// if table is excluded, return error and skip parsing event or dump
	if !c.checkTableMatch(db, table) {
		return nil, xerrors.Errorf("%w", ErrExcludedTable)
	}
	c.tableLock.RLock()
	t, ok := c.tables[key]
	c.tableLock.RUnlock()

	if ok {
		return t, nil
	}

	if c.cfg.DiscardNoMetaRowEvent {
		c.tableLock.RLock()
		lastTime, ok := c.errorTablesGetTime[key]
		c.tableLock.RUnlock()
		if ok && time.Since(lastTime) < UnknownTableRetryPeriod {
			return nil, xerrors.Errorf("%w", schema.ErrMissingTableMeta)
		}
	}

	t, err := schema.NewTable(c, db, table)
	if err != nil {
		// check table not exists
		if ok, err1 := schema.IsTableExist(c, db, table); err1 == nil && !ok {
			return nil, xerrors.Errorf("%w", schema.ErrTableNotExist)
		}
		// work around : RDS HAHeartBeat
		// ref : https://github.com/alibaba/canal/blob/master/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L385
		// issue : https://github.com/alibaba/canal/issues/222
		// This is a common error in RDS that canal can't get HAHealthCheckSchema's meta, so we mock a table meta.
		// If canal just skip and log error, as RDS HA heartbeat interval is very short, so too many HAHeartBeat errors will be logged.
		if key == schema.HAHealthCheckSchema {
			// mock ha_health_check meta
			ta := &schema.Table{
				Schema:  db,
				Name:    table,
				Columns: make([]schema.TableColumn, 0, 2),
				Indexes: make([]*schema.Index, 0),
			}
			ta.AddColumn("id", "bigint(20)", "", "")
			ta.AddColumn("type", "char(1)", "", "")
			c.tableLock.Lock()
			c.tables[key] = ta
			c.tableLock.Unlock()
			return ta, nil
		}
		// if DiscardNoMetaRowEvent is true, we just log this error
		if c.cfg.DiscardNoMetaRowEvent {
			c.tableLock.Lock()
			c.errorTablesGetTime[key] = time.Now()
			c.tableLock.Unlock()
			c.logger.Errorf("canal get table meta err: %v", err)
			return nil, xerrors.Errorf("%w", schema.ErrMissingTableMeta)
		}
		return nil, xerrors.Errorf("failed to get table: %w", err)
	}
	c.logTable(t)
	if c.cfg.FailOnDecimal {
		if decimalColumnIndex := findDecimalColumn(t); decimalColumnIndex != -1 {
			return nil, xerrors.Errorf("table %q contains column %q of type %s. Columns of decimal types currently are not supported. Please exclude the table from the transfer", t.Name, t.Columns[decimalColumnIndex].Name, t.Columns[decimalColumnIndex].RawType)
		}
	}

	if len(t.PKColumns) == 0 || len(t.PKColumns) > 1 {
		// load unique and primary keys constraints
		constraints, err := c.loadTableConstraints(db, table)
		if err != nil {
			return nil, xerrors.Errorf("Unable to load contraints for table without primary key %v.%v: %w", db, table, err)
		}
		colsIdx := make(map[string]int)
		for i, c := range t.Columns {
			colsIdx[c.Name] = i
		}

		var constraintColumns []string
		var constraintName string
		if len(t.PKColumns) == 0 {
			// use any uniq constraint as primary key
			c.logger.Info("use any uniq constraint as primary key")
			for name, cols := range constraints {
				c.logger.Infof("use %v(%v) constraint as primary key for %v.%v", name, cols, db, table)
				constraintName = name
				constraintColumns = cols
				break
			}
		} else {
			// get order for composite primary key
			for name, cols := range constraints {
				if strings.ToLower(name) == "primary" {
					if len(cols) != len(t.PKColumns) {
						c.logger.Warnf("length of loaded primary constraint(%v = %v) differs with schema(columns(%v), pkeys(%v) = %v)", cols, len(cols), t.Columns, t.PKColumns, len(t.PKColumns))
						break
					}
					t.PKColumns = make([]int, 0)
					constraintName = name
					constraintColumns = cols
					break
				}
			}
		}

		for _, colName := range constraintColumns {
			idx, ok := colsIdx[colName]
			if !ok {
				return nil, xerrors.Errorf("cannot use unknown column %v as part of primary key, table = %v.%v, constraint name = %v", colName, db, table, constraintName)
			}
			t.PKColumns = append(t.PKColumns, idx)
		}
	}

	c.tableLock.Lock()
	c.tables[key] = t
	if c.cfg.DiscardNoMetaRowEvent {
		// if get table info success, delete this key from errorTablesGetTime
		delete(c.errorTablesGetTime, key)
	}
	c.tableLock.Unlock()

	return t, nil
}

func (c *Canal) logTable(table *schema.Table) {
	marshaledTable, _ := json.Marshal(table)
	c.logger.Info(fmt.Sprintf("got table schema, tableName: %s", table.String()), log.ByteString("table", marshaledTable))
}

func (c *Canal) loadTableConstraints(db, table string) (map[string][]string, error) {
	connConf := mysql_driver.NewConfig()
	connConf.Addr = c.cfg.Addr
	connConf.User = c.cfg.User
	connConf.Passwd = c.cfg.Password
	connConf.DBName = db
	connConf.Net = "tcp"
	if c.cfg.TLSConfig != nil {
		connConf.TLSConfig = "custom"
		if err := mysql_driver.RegisterTLSConfig("custom", c.cfg.TLSConfig); err != nil {
			return nil, xerrors.Errorf("unable to set tls: %w", err)
		}
	}
	connector, err := mysql_driver.NewConnector(connConf)
	if err != nil {
		return nil, xerrors.Errorf("unable to init connector to source storage: %w", err)
	}

	dbConn := sql.OpenDB(connector)
	defer func() {
		if err := dbConn.Close(); err != nil {
			logger.Log.Errorf("Unable to close storage db connector: %v", err)
		}
	}()
	return LoadTableConstraints(dbConn, abstract.TableID{Namespace: db, Name: table})
}

// ClearTableCache clear table cache
func (c *Canal) ClearTableCache(db []byte, table []byte) {
	key := fmt.Sprintf("%s.%s", db, table)
	c.tableLock.Lock()
	delete(c.tables, key)
	if c.cfg.DiscardNoMetaRowEvent {
		delete(c.errorTablesGetTime, key)
	}
	c.tableLock.Unlock()
}

// CheckBinlogRowImage checks MySQL binlog row image, must be in FULL, MINIMAL, NOBLOB
func (c *Canal) CheckBinlogRowImage(image string) error {
	// need to check MySQL binlog row image? full, minimal or noblob?
	// now only log
	if c.cfg.Flavor == mysql.MySQLFlavor {
		if res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_row_image"`); err != nil {
			return xerrors.Errorf("failed to execute SQL SHOW GLOBAL VARIABLES: %w", err)
		} else {
			// MySQL has binlog row image from 5.6, so older will return empty
			rowImage, _ := res.GetString(0, 1)
			if rowImage != "" && !strings.EqualFold(rowImage, image) {
				return xerrors.Errorf("MySQL uses %s binlog row image, but we want %s", rowImage, image)
			}
		}
	}

	return nil
}

func (c *Canal) checkBinlogRowFormat() error {
	res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return err
	} else if f, _ := res.GetString(0, 1); f != "ROW" {
		return abstract.NewFatalError(xerrors.Errorf("Expected binlog_format to be 'ROW' but got '%s'", f))
	}
	return nil
}

func (c *Canal) prepareSyncer() error {
	cfg := replication.BinlogSyncerConfig{
		ServerID:                c.cfg.ServerID,
		Flavor:                  c.cfg.Flavor,
		User:                    c.cfg.User,
		Password:                c.cfg.Password,
		Charset:                 c.cfg.Charset,
		HeartbeatPeriod:         c.cfg.HeartbeatPeriod,
		ReadTimeout:             c.cfg.ReadTimeout,
		UseDecimal:              c.cfg.UseDecimal,
		ParseTime:               c.cfg.ParseTime,
		TimestampStringLocation: c.cfg.TimestampStringLocation,
		SemiSyncEnabled:         c.cfg.SemiSyncEnabled,
		MaxReconnectAttempts:    c.cfg.MaxReconnectAttempts,
		TLSConfig:               c.cfg.TLSConfig,
	}

	if strings.Contains(c.cfg.Addr, "/") {
		cfg.Host = c.cfg.Addr
	} else if strings.HasPrefix(c.cfg.Addr, "[") && strings.Contains(c.cfg.Addr, "]:") {
		// addr is ipv6
		seps := strings.Split(c.cfg.Addr, ":")
		port, err := strconv.ParseUint(seps[len(seps)-1], 10, 16)
		if err != nil {
			return xerrors.Errorf("failed to parse network port number: %w", err)
		}
		cfg.Port = uint16(port)
		cfg.Host = strings.Join(seps[:len(seps)-1], ":")
	} else {
		seps := strings.Split(c.cfg.Addr, ":")
		if len(seps) != 2 {
			return xerrors.Errorf("invalid mysql addr format %s, must host:port", c.cfg.Addr)
		}

		port, err := strconv.ParseUint(seps[1], 10, 16)
		if err != nil {
			return xerrors.Errorf("failed to parse network port number: %w", err)
		}

		cfg.Host = seps[0]
		cfg.Port = uint16(port)
	}

	c.syncer = replication.NewBinlogSyncer(cfg)

	return nil
}

// Execute a SQL
func (c *Canal) Execute(cmd string, args ...interface{}) (*mysql.Result, error) {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if c.conn == nil {
			argF := make([]func(*client.Conn), 0)
			if c.cfg.TLSConfig != nil {
				argF = append(argF, func(conn *client.Conn) {
					conn.SetTLSConfig(c.cfg.TLSConfig)
				})
			}
			var err error
			c.conn, err = client.Connect(c.cfg.Addr, c.cfg.User, c.cfg.Password, "", argF...)
			if err != nil {
				return nil, xerrors.Errorf("failed to connect to the database: %w", err)
			}
		}

		rr, err := c.conn.Execute(cmd, args...)
		if err != nil {
			if mysql.ErrorEqual(err, mysql.ErrBadConn) {
				_ = c.conn.Close()
				c.conn = nil
				continue
			}
			return nil, xerrors.Errorf("failed to execute SQL: %w", err)
		}
		return rr, nil
	}
	return nil, xerrors.Errorf("failed to execute SQL: bad connection, number of retries (%d) exceeded", retryNum)
}

func (c *Canal) SyncedPosition() mysql.Position {
	return c.master.Position()
}

func (c *Canal) SyncedTimestamp() uint32 {
	return c.master.timestamp
}

func (c *Canal) SyncedGTIDSet() mysql.GTIDSet {
	return c.master.GTIDSet()
}
