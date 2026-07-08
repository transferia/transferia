// Package ch
// cluster - it's like stand-alone cluster with multimaster
// []*SinkServer - masters (AltHosts). We don't care in which SinkServer we are writing - it's like multimaster.
// The live set of servers and round-robin over them is managed by serverPool.
package clickhouse

import (
	"context"
	stderrors "errors"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/ptr"
	"github.com/transferia/transferia/pkg/abstract"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	clickhouse_errors "github.com/transferia/transferia/pkg/providers/clickhouse/errors"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type sinkCluster struct {
	logger   log.Logger
	config   clickhouse_model.ChSinkClusterParams
	topology *topology.Topology
	pool     *serverPool

	distributedDDLMu      sync.Mutex
	distributedDDLEnabled *bool // distributedDDLEnabled == nil means that mode is not yet determined.
}

func (c *sinkCluster) bestSinkServer() *SinkServer {
	return c.pool.best()
}

type TableSpec struct {
	Name   string
	Schema *abstract.TableSchema
}

func (c *sinkCluster) Insert(spec *TableSpec, rows []abstract.ChangeItem) error {
	return c.pool.best().Insert(spec, rows)
}

func (c *sinkCluster) TruncateTable(tableName string) error {
	ctx := context.TODO()
	if c.perHostDDL() {
		var errs []error
		for _, ss := range c.pool.serversCopy() {
			if err := ss.TruncateTable(ctx, tableName, false); err != nil {
				errs = append(errs, err)
			}
		}
		if err := stderrors.Join(errs...); err != nil {
			return xerrors.Errorf("cannot truncate table in per host style: %w", err)
		}

		return nil
	}

	return c.execDDL(func(distributed bool) error {
		if err := c.pool.best().TruncateTable(ctx, tableName, distributed); err != nil {
			return xerrors.Errorf("cannot truncate table (distributed=%v): %w", distributed, err)
		}
		return nil
	})
}

func (c *sinkCluster) DropTable(tableName string) error {
	ctx := context.TODO()
	if c.perHostDDL() {
		var errs []error
		for _, server := range c.pool.serversCopy() {
			if err := server.DropTable(ctx, tableName, false); err != nil {
				errs = append(errs, err)
			}
		}
		if err := stderrors.Join(errs...); err != nil {
			return xerrors.Errorf("cannot drop table in per host style: %w", err)
		}
		return nil
	}

	return c.execDDL(func(distributed bool) error {
		if err := c.pool.best().DropTable(ctx, tableName, distributed); err != nil {
			return xerrors.Errorf("cannot drop table (distributed=%v): %w", distributed, err)
		}
		return nil
	})
}

func (c *sinkCluster) execDDL(executor func(distributed bool) error) error {
	c.distributedDDLMu.Lock()

	if c.distributedDDLEnabled == nil && (c.topology.ClusterName() == "" || c.topology.SingleNode()) {
		if !c.topology.SingleNode() {
			c.distributedDDLMu.Unlock()
			return xerrors.Errorf("resolved empty cluster name for non-single-node cluster")
		}
		c.logger.Warn("cluster name is empty, disabling distributed DDL")
		c.distributedDDLEnabled = ptr.Bool(false)
	}

	if c.distributedDDLEnabled != nil {
		c.distributedDDLMu.Unlock()
		if err := executor(*c.distributedDDLEnabled); err != nil {
			return xerrors.Errorf("error executing DDL (distributed=%v): %w", *c.distributedDDLEnabled, err)
		}
		return nil
	}

	defer c.distributedDDLMu.Unlock()
	err := executor(true)
	if err == nil {
		c.distributedDDLEnabled = ptr.Bool(true)
		c.logger.Info("distributed DDL is enabled")
		return nil
	}

	if !clickhouse_errors.IsDistributedDDLError(err) {
		return xerrors.Errorf("error executing DDL: %w", err)
	}
	c.logger.Error("Got distributed DDL error", log.Error(err))

	if !c.topology.SingleNode() {
		c.logger.Error("cluster is not single node and distributed DDL is not available")
		return clickhouse_errors.ForbiddenDistributedDDLError
	}

	if err := executor(false); err != nil {
		return xerrors.Errorf("error executing DDL: %w", err)
	}
	c.logger.Warn("disabling distributed DDL for cluster")
	c.distributedDDLEnabled = ptr.Bool(false)
	return nil
}

func (c *sinkCluster) perHostDDL() bool {
	return c.config.ShardByTransferID()
}

func (c *sinkCluster) Close() error {
	return c.pool.close()
}

func (c *sinkCluster) RemoveOldParts(keepPartCount int, table string) error {
	for _, s := range c.pool.serversCopy() {
		if err := s.CleanupPartitions(keepPartCount, table); err != nil {
			return err
		}
	}
	return nil
}

func newSinkCluster(config clickhouse_model.ChSinkClusterParams, lgr log.Logger, metrics *stats.ChStats, topology *topology.Topology) (*sinkCluster, error) {
	sinkCl := &sinkCluster{
		logger:                lgr,
		config:                config,
		topology:              topology,
		pool:                  nil,
		distributedDDLMu:      sync.Mutex{},
		distributedDDLEnabled: nil,
	}

	newServer := func(host *conn_clickhouse.Host) (*SinkServer, error) {
		return NewSinkServer(config.MakeChildServerParams(host), lgr, metrics, sinkCl)
	}
	pool, err := newServerPool(config.AltHosts(), newServer, defaultReviveInterval, lgr)
	if err != nil {
		return nil, err
	}
	sinkCl.pool = pool

	return sinkCl, nil
}
