// Package ch
//
// SinkServer - it's like master (in multi-master system) destination
package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/blang/semver/v4"
	"github.com/cenkalti/backoff/v4"
	"github.com/jmoiron/sqlx"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/errors/coded"
	"github.com/transferria/transferria/pkg/providers"
	"github.com/transferria/transferria/pkg/providers/clickhouse/conn"
	"github.com/transferria/transferria/pkg/providers/clickhouse/errors"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
	"github.com/transferria/transferria/pkg/stats"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type SinkServer struct {
	db            *sql.DB
	logger        log.Logger
	host          string
	metrics       *stats.ChStats
	config        model.ChSinkServerParams
	getTableMutex sync.Mutex
	tables        map[string]*sinkTable
	closeCh       chan struct{}
	onceClose     sync.Once
	alive         bool
	lastFail      time.Time
	callbacks     *SinkServerCallbacks // special callback, used only in test
	cluster       *sinkCluster
	version       semver.Version
}

type SinkServerCallbacks struct {
	OnPing func(sinkServer *SinkServer)
}

func (s *SinkServer) TestSetCallbackOnPing(onPing *SinkServerCallbacks) {
	s.callbacks = onPing
}

func (s *SinkServer) Close() error {
	close(s.closeCh)
	if err := s.db.Close(); err != nil {
		s.logger.Warn("failed to close db", log.Error(err))
	}
	return nil
}

func (s *SinkServer) mergeQ() {
	for {
		select {
		case <-s.closeCh:
			return
		default:
		}
		s.ping()
		time.Sleep(10 * time.Second)
	}
}

func (s *SinkServer) ping() {
	if s.callbacks != nil {
		s.callbacks.OnPing(s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), errors.ClickhouseReadTimeout)
	defer cancel()
	row := s.db.QueryRowContext(ctx, `select 1+1;`)
	var q int
	err := row.Scan(&q)
	if err == nil {
		s.logger.Debug("Host alive")
		s.alive = true
	} else {
		s.logger.Warn("Ping error", log.Error(err))
		s.alive = false
	}
}

func (s *SinkServer) TruncateTable(ctx context.Context, tableName string, onCluster bool) error {
	ddl := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", s.tableReferenceForDDL(tableName, onCluster))
	return s.ExecDDL(ctx, ddl)
}

func (s *SinkServer) DropTable(ctx context.Context, tableName string, onCluster bool) error {
	ddl := fmt.Sprintf("DROP TABLE IF EXISTS %s NO DELAY", s.tableReferenceForDDL(tableName, onCluster))
	return s.ExecDDL(ctx, ddl)
}

func (s *SinkServer) isOnClusterDDL() bool {
	return len(s.cluster.topology.ClusterName()) > 0
}

func (s *SinkServer) tableReferenceForDDL(tableName string, onCluster bool) string {
	cluster := ""
	if onCluster && s.isOnClusterDDL() {
		cluster = fmt.Sprintf(" ON CLUSTER `%s`", s.cluster.topology.ClusterName())
	}
	return fmt.Sprintf("`%s`.`%s`%s", s.config.Database(), tableName, cluster)
}

func (s *SinkServer) ExecDDL(ctx context.Context, ddl string) error {
	timeout, err := s.queryDistributedDDLTimeout()
	if err != nil {
		s.logger.Warn("Error reading DDL timeout, using default value", log.Error(err))
		timeout = errors.ClickhouseDDLTimeout
	}
	s.logger.Infof("Using DDL Timeout %d seconds", timeout)
	err = backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout+errors.DDLTimeoutCorrection)*time.Second)
		defer cancel()
		_, err := s.db.ExecContext(ctx, ddl)
		if err != nil {
			if errors.IsFatalClickhouseError(err) {
				//nolint:descriptiveerrors
				return backoff.Permanent(abstract.NewFatalError(err))
			}
			s.logger.Warnf("failed to execute DDL %q: %v", ddl, err)
			if ddlErr := errors.AsDistributedDDLTimeout(err); ddlErr != nil {
				s.logger.Warn("Got distributed DDL timeout, skipping retries")
				err = backoff.Permanent(ddlErr)
			}
		}
		//nolint:descriptiveerrors
		return err
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))

	if err == nil {
		return nil
	}

	if e, ok := err.(*errors.ErrDistributedDDLTimeout); ok {
		taskPath := e.ZKTaskPath
		err = s.checkDDLTask(taskPath)
	}

	return err
}

func (s *SinkServer) checkDDLTask(taskPath string) error {
	s.logger.Warnf("Checking DDL task %s", taskPath)
	ctx, cancel := context.WithTimeout(context.Background(), errors.ClickhouseReadTimeout*2)
	defer cancel()
	hostRows, err := s.db.QueryContext(ctx, `SELECT name FROM system.zookeeper WHERE path = ?`, taskPath+"/finished")
	if err != nil {
		return xerrors.Errorf("error executing DDL task result: %w", err)
	}
	defer hostRows.Close()
	var hosts []string
	for hostRows.Next() {
		var host string
		if err = hostRows.Scan(&host); err != nil {
			return xerrors.Errorf("error scanning DDL task hosts: %w", err)
		}

		host = strings.Split(host, ":")[0]  // remove port from host URL
		host, err = url.QueryUnescape(host) // for some reason in MDB ZK hosts are presented in URL Encoded format (i.e. "vla%2Dgi40aoy0s1yyeu4u%2Edb%2Eyandex%2Enet")
		if err != nil {
			return xerrors.Errorf("unable to extract host name from %s: %w", host, err)
		}
		hosts = append(hosts, host)
	}
	if err := hostRows.Err(); err != nil {
		return xerrors.Errorf("error reading DDL task hosts: %w", err)
	}
	s.logger.Info(fmt.Sprintf("Got hosts with finished DDL task %s", taskPath), log.Array("hosts", hosts))

	var totalShards, execShards int
	shardQ, args, err := sqlx.In(`
		SELECT uniqExact(shard_num) as total_shards, uniqExactIf(shard_num, host_name IN (?)) as exec_shards
		FROM system.clusters
		WHERE cluster = ?`, hosts, s.cluster.topology.ClusterName())
	if err != nil {
		return xerrors.Errorf("error building shards query: %w", err)
	}

	err = s.db.QueryRowContext(ctx, shardQ, args...).Scan(&totalShards, &execShards)
	if err != nil {
		return xerrors.Errorf("error reading cluster shards number: %w", err)
	}

	s.logger.Infof("DDL task %s is executed on %d shards of %d", taskPath, execShards, totalShards)
	if totalShards != execShards {
		return errors.DDLTaskError{
			ExecShards:  execShards,
			TotalShards: totalShards,
		}
	}
	return nil
}

func (s *SinkServer) queryDistributedDDLTimeout() (int, error) {
	var result int
	err := s.QuerySingleValue("SELECT value FROM system.settings WHERE name = 'distributed_ddl_task_timeout'", &result)
	return result, err
}

func (s *SinkServer) QuerySingleValue(query string, target interface{}) error {
	return backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), errors.ClickhouseReadTimeout)
		defer cancel()
		if err := s.db.QueryRowContext(ctx, query).Scan(target); err != nil {
			if errors.IsFatalClickhouseError(err) {
				return backoff.Permanent(err)
			}
			return xerrors.Errorf("query error: %w", err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))
}

func (s *SinkServer) Insert(spec *TableSpec, rows []abstract.ChangeItem) error {
	t, err := s.GetTable(spec.Name, spec.Schema)
	if err != nil {
		s.lastFail = time.Now()
		return xerrors.Errorf("unable to get table '%s': %w", spec.Name, err)
	}

	if err := t.ApplyChangeItems(rows); err != nil {
		s.lastFail = time.Now()
		return xerrors.Errorf("unable to apply changes for table '%s': %w", spec.Name, err)
	}
	return nil
}

func (s *SinkServer) GetTable(table string, schema *abstract.TableSchema) (*sinkTable, error) {
	s.getTableMutex.Lock()
	defer s.getTableMutex.Unlock()

	if s.tables[table] != nil {
		return s.tables[table], nil
	}

	tbl := &sinkTable{
		server:          s,
		tableName:       normalizeTableName(table),
		config:          s.config,
		logger:          log.With(s.logger, log.Any("table", normalizeTableName(table))),
		colTypes:        nil,
		cols:            nil,
		metrics:         s.metrics,
		avgRowSize:      0,
		cluster:         s.cluster,
		timezoneFetched: false,
		timezone:        nil,
		version:         s.version,
	}

	if err := tbl.resolveTimezone(); err != nil {
		return nil, xerrors.Errorf("failed to resolve CH cluster timezone: %w", err)
	}

	if err := tbl.Init(schema); err != nil {
		s.logger.Error("Unable to init table", log.Error(err), log.Any("table", table), log.Any("schema", schema))
		return nil, err
	}

	s.tables[table] = tbl
	return s.tables[table], nil
}

func (s *SinkServer) Alive() bool {
	return s.alive && time.Since(s.lastFail).Minutes() > 5
}

func (s *SinkServer) CleanupPartitions(keepParts int, table string) error {
	rows, err := s.db.Query(`SELECT
    table,
    partition,
    formatReadableSize(sum(bytes)) AS size
FROM system.parts
where table = ?
GROUP BY
    table,
    partition
ORDER BY
    table ASC,
    partition DESC`, table)
	if err != nil {
		return xerrors.Errorf("unable to query partitions: %w", err)
	}
	type partRow struct {
		table, partition, size string
	}
	parts := make([]partRow, 0)
	for rows.Next() {
		var table, partition, size string
		if err := rows.Scan(&table, &partition, &size); err != nil {
			return xerrors.Errorf("unable to read row: %w", err)
		}
		parts = append(parts, partRow{
			table:     table,
			partition: partition,
			size:      size,
		})
	}
	s.logger.Infof("rotator found %v parts for table %v from %v", len(parts), table, *s.config.Host())
	if len(parts) > keepParts {
		oldParts := parts[keepParts:]
		s.logger.Infof("prepare to delete %v parts for table %v", len(oldParts), table)
		for _, part := range oldParts {
			dropQ := fmt.Sprintf("ALTER TABLE `%v` DROP PARTITION '%v'", table, part.partition)
			if _, err := s.db.Exec(dropQ); err != nil {
				return xerrors.Errorf("unable to exec drop part: %w", err)
			}
			s.logger.Infof("delete part %v (%v)", part.partition, part.size)
		}
	}
	return nil
}

func NewSinkServerImplWithVersion(
	cfg model.ChSinkServerParams,
	lgr log.Logger,
	metrics *stats.ChStats,
	cluster *sinkCluster,
	version semver.Version,
) (*SinkServer, error) {
	host := *cfg.Host()
	db, err := conn.ConnectNative(host, cfg)
	if err != nil {
		return nil, xerrors.Errorf("native connection error: %w", err)
	}

	s := &SinkServer{
		db:            db,
		logger:        log.With(lgr, log.String("ch_host", host)),
		host:          host,
		metrics:       metrics,
		config:        cfg,
		getTableMutex: sync.Mutex{},
		tables:        map[string]*sinkTable{},
		closeCh:       make(chan struct{}),
		onceClose:     sync.Once{},
		alive:         false,
		lastFail:      time.Time{},
		callbacks:     nil,
		cluster:       cluster,
		version:       version,
	}

	ctx, cancel := context.WithTimeout(context.Background(), errors.ClickhouseReadTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		var exception *clickhouse.Exception
		if xerrors.As(err, &exception) {
			lgr.Error("CH ping error", log.Any("exception", exception))
			if errors.IsFatalClickhouseError(err) {
				return nil, abstract.NewFatalError(coded.Errorf(providers.NetworkUnreachable, "unable to init ch sink-server, fatal error: %w", err))
			}
			s.alive = true
		} else {
			lgr.Error("Not CH error", log.Error(err), log.Any("db_host", host))
			s.alive = false
		}
	} else {
		s.alive = true
	}

	return s, nil
}

func NewSinkServerImpl(cfg model.ChSinkServerParams, lgr log.Logger, metrics *stats.ChStats, cluster *sinkCluster) (*SinkServer, error) {
	version, err := backoff.RetryNotifyWithData(func() (string, error) {
		host := *cfg.Host()
		db, err := conn.ConnectNative(host, cfg)
		if err != nil {
			return "", xerrors.Errorf("native connection error: %w", err)
		}

		var version string

		if err := db.QueryRow("select version();").Scan(&version); err != nil {
			if errors.IsFatalClickhouseError(err) {
				return "", backoff.Permanent(xerrors.Errorf("unable to select clickhouse version: %w", err))
			}
			return "", xerrors.Errorf("unable to select clickhouse version: %w", err)
		}
		return version, nil
	}, backoff.WithMaxRetries(util.NewExponentialBackOff(), 5), util.BackoffLoggerWarn(lgr, "version resolver"))
	if err != nil {
		return nil, xerrors.Errorf("unable to extract version: %w", err)
	}

	parsedVersion, err := parseSemver(version)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse semver: %w", err)
	}

	s, err := NewSinkServerImplWithVersion(cfg, lgr, metrics, cluster, *parsedVersion)
	if err != nil {
		return nil, xerrors.Errorf("unable to build: %w", err)
	}
	return s, nil
}

func (s *SinkServer) RunGoroutines() {
	go s.mergeQ()
}

func NewSinkServer(cfg model.ChSinkServerParams, lgr log.Logger, metrics *stats.ChStats, cluster *sinkCluster) (*SinkServer, error) {
	s, err := NewSinkServerImpl(cfg, lgr, metrics, cluster)
	if err != nil {
		return nil, err
	}
	s.RunGoroutines()
	return s, nil
}
