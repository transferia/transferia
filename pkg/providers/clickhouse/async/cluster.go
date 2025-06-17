//go:build !disable_clickhouse_provider

package async

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/cenkalti/backoff/v4"
	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/library/go/ptr"
	"github.com/transferia/transferia/pkg/abstract"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
	db_model "github.com/transferia/transferia/pkg/providers/clickhouse/async/model/db"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/providers/clickhouse/errors"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	topology2 "github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/exp/maps"
)

// TODO: refactor and decouple client and implementation

type DDLStreamingClient interface {
	db_model.Client
	db_model.DDLExecutor
	db_model.StreamInserter
	io.Closer
}

type ShardClient interface {
	db_model.Client
	db_model.DDLExecutor
	AliveHost() (DDLStreamingClient, error)
	io.Closer
}

type ClusterClient interface {
	db_model.Client
	db_model.DDLExecutor
	sharding.Shards[ShardClient]
	io.Closer
}

type shardClient struct {
	db                    *sql.DB
	opts                  *clickhouse.Options
	distributedDDLEnabled *bool
	lgr                   log.Logger
	topology              *topology2.Topology
	hostIterator          int
}

func (s *shardClient) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, query, args...)
}

func (s *shardClient) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, query, args...)
}

func (s *shardClient) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, query, args...)
}

func (s *shardClient) Close() error {
	return s.db.Close()
}

func (s *shardClient) execDistributedDDL(ctx context.Context, ddl string) error {
	timeout, err := s.queryDistributedDDLTimeout(ctx)
	if err != nil {
		s.lgr.Warn("Error reading DDL timeout, using default value", log.Error(err))
		timeout = errors.ClickhouseDDLTimeout
	}
	s.lgr.Infof("Using DDL Timeout %d seconds", timeout)

	err = backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout+errors.DDLTimeoutCorrection)*time.Second)
		defer cancel()
		_, err := s.db.ExecContext(ctx, ddl)
		if err != nil {
			if errors.IsFatalClickhouseError(err) {
				//nolint:descriptiveerrors
				return backoff.Permanent(abstract.NewFatalError(err))
			}
			s.lgr.Warnf("failed to execute DDL %q: %v", ddl, err)
			if ddlErr := errors.AsDistributedDDLTimeout(err); ddlErr != nil {
				s.lgr.Warn("Got distributed DDL timeout, skipping retries")
				//nolint:descriptiveerrors
				return backoff.Permanent(ddlErr)
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

func (s *shardClient) checkDDLTask(taskPath string) error {
	s.lgr.Warnf("Checking DDL task %s", taskPath)
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
	s.lgr.Info(fmt.Sprintf("Got hosts with finished DDL task %s", taskPath), log.Array("hosts", hosts))

	var totalShards, execShards int
	shardQ, args, err := sqlx.In(`
		SELECT uniqExact(shard_num) as total_shards, uniqExactIf(shard_num, host_name IN (?)) as exec_shards
		FROM system.clusters
		WHERE cluster = ?`, hosts, s.topology.ClusterName())
	if err != nil {
		return xerrors.Errorf("error building shards query: %w", err)
	}

	err = s.db.QueryRowContext(ctx, shardQ, args...).Scan(&totalShards, &execShards)
	if err != nil {
		return xerrors.Errorf("error reading cluster shards number: %w", err)
	}

	s.lgr.Infof("DDL task %s is executed on %d shards of %d", taskPath, execShards, totalShards)
	if (totalShards != execShards) || (totalShards == 0) {
		return errors.MakeDDLTaskError(execShards, totalShards)
	}
	return nil
}

func (s *shardClient) queryDistributedDDLTimeout(ctx context.Context) (int, error) {
	var result int
	err := s.db.QueryRowContext(ctx, "SELECT value FROM system.settings WHERE name = 'distributed_ddl_task_timeout'").Scan(&result)
	return result, err
}

func (s *shardClient) ExecDDL(fn db_model.DDLFactory) error {
	// TODO: probably shard client should not support cluster ddl or should not be ddl client at all
	ctx := context.TODO()
	if s.distributedDDLEnabled != nil {
		q, err := fn(*s.distributedDDLEnabled, s.topology.ClusterName())
		if err != nil {
			return xerrors.Errorf("error getting DDL query: %w", err)
		}
		if *s.distributedDDLEnabled {
			err = s.execDistributedDDL(ctx, q)
		} else {
			_, err = s.db.ExecContext(ctx, q)
		}
		if err != nil {
			return xerrors.Errorf("error executing DDL (distributed=%v): %w", *s.distributedDDLEnabled, err)
		}
		return nil
	}

	q, err := fn(true, s.topology.ClusterName())
	if err != nil {
		return xerrors.Errorf("error getting DDL query: %w", err)
	}

	if err = s.execDistributedDDL(ctx, q); err == nil {
		s.distributedDDLEnabled = ptr.Bool(true)
		return nil
	}
	if !errors.IsDistributedDDLError(err) {
		return xerrors.Errorf("error executing distributed DDL: %w", err)
	}

	q, err = fn(false, s.topology.ClusterName())
	if err != nil {
		return xerrors.Errorf("error getting DDL query: %w", err)
	}
	if _, err = s.db.ExecContext(ctx, q); err != nil {
		return xerrors.Errorf("error executing non-distributed DDL: %w", err)
	}
	s.distributedDDLEnabled = ptr.Bool(false)
	return nil
}

// AliveHost returns one of alive hosts. Hosts selection order is determined by the Round-robin algorithm.
func (s *shardClient) AliveHost() (DDLStreamingClient, error) {
	opts := *s.opts
	for i := 0; i < len(s.opts.Addr); i++ {
		opts.Addr = []string{s.nextHostAddr()}
		cl, err := NewHostClient(&opts, log.With(s.lgr, log.String("shardHost", opts.Addr[0])))
		if err != nil {
			s.lgr.Warn("Error getting host client", log.String("host", opts.Addr[0]), log.Error(err))
		} else {
			return cl, nil
		}
	}
	return nil, xerrors.New("no alive hosts found for shard")
}

func (s *shardClient) nextHostAddr() string {
	s.hostIterator = (s.hostIterator + 1) % len(s.opts.Addr)
	return s.opts.Addr[s.hostIterator]
}

func NewShardClient(hosts []*chconn.Host, cp conn.ConnParams, topology *topology2.Topology, lgr log.Logger) (ShardClient, error) {
	opts, err := conn.GetClickhouseOptions(cp, hosts)
	if err != nil {
		return nil, err
	}

	shardDB := clickhouse.OpenDB(opts)
	var distrDDL *bool
	if topology.SingleNode() && topology.ClusterName() == "" {
		distrDDL = ptr.Bool(false)
	}

	return &shardClient{
		db:                    shardDB,
		opts:                  opts,
		distributedDDLEnabled: distrDDL,
		lgr:                   lgr,
		topology:              topology,
		hostIterator:          rand.Intn(len(opts.Addr)),
	}, nil
}

type clusterClient struct {
	sharding.ShardMap[ShardClient]
	lgr log.Logger
}

func (c *clusterClient) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.randomShard().QueryContext(ctx, query, args...)
}

func (c *clusterClient) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.randomShard().QueryRowContext(ctx, query, args...)
}

func (c *clusterClient) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.randomShard().ExecContext(ctx, query, args...)
}

func (c *clusterClient) ExecDDL(fn db_model.DDLFactory) error {
	return c.randomShard().ExecDDL(fn)
}

func (c *clusterClient) Close() error {
	var errs error
	for shardID, shard := range c.ShardMap {
		c.lgr.Debugf("clusterClient: closing shard %d", shardID)
		errs = multierr.Append(errs, shard.Close())
	}
	return errs
}

func (c *clusterClient) randomShard() ShardClient {
	idx := rand.Intn(len(c.ShardMap))
	k := maps.Keys(c.ShardMap)[idx]
	return c.Shard(k)
}

func NewClusterClient(conn conn.ConnParams, topology *topology2.Topology, shards sharding.ShardMap[[]*chconn.Host], lgr log.Logger) (ClusterClient, error) {
	clients := make(sharding.ShardMap[ShardClient])
	for shard, hosts := range shards {
		cl, err := NewShardClient(hosts, conn, topology, log.With(lgr, log.String("shardID", fmt.Sprint(shard))))
		if err != nil {
			return nil, xerrors.Errorf("error making shard client for shard %v: %w", shard, err)
		}
		clients[shard] = cl
	}
	return &clusterClient{ShardMap: clients, lgr: lgr}, nil
}

type hostClient struct {
	db   *sql.DB
	opts *clickhouse.Options
	lgr  log.Logger
}

func (h *hostClient) StreamInsert(query string, marshaller db_model.ChangeItemMarshaller) (db_model.Streamer, error) {
	return newCHV2Streamer(h.opts, query, marshaller, h.lgr)
}

func (h *hostClient) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return h.db.QueryContext(ctx, query, args...)
}

func (h *hostClient) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return h.db.QueryRowContext(ctx, query, args...)
}

func (h *hostClient) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return h.db.ExecContext(ctx, query, args...)
}

func (h *hostClient) ExecDDL(fn db_model.DDLFactory) error {
	q, err := fn(false, "")
	if err != nil {
		return xerrors.Errorf("error getting DDL query: %w", err)
	}
	_, err = h.db.ExecContext(context.Background(), q)
	return err
}

func (h *hostClient) Close() error {
	return h.db.Close()
}

func NewHostClient(opts *clickhouse.Options, lgr log.Logger) (DDLStreamingClient, error) {
	hostDB := clickhouse.OpenDB(opts)
	if err := hostDB.Ping(); err != nil {
		_ = hostDB.Close()
		return nil, xerrors.Errorf("host %s seems to be dead, ping failed: %w", opts.Addr[0], err)
	}
	return &hostClient{
		db:   hostDB,
		opts: opts,
		lgr:  lgr,
	}, nil
}
