//go:build !disable_clickhouse_provider

package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/ptr"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/events"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/providers/clickhouse/errors"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema"
	"github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/exp/maps"
)

type HTTPTarget struct {
	client         httpclient.HTTPClient
	config         model.ChSinkParams
	logger         log.Logger
	cluster        *topology.Cluster
	altNames       map[string]string
	metrics        *stats.SinkerStats
	wrapperMetrics *stats.WrapperStats

	distributedDDLMu      sync.Mutex
	distributedDDLEnabled *bool
}

var syntaxErrorRegexp = regexp.MustCompile(`^.*\(at row ([0-9]+)\).*$`)

func (c *HTTPTarget) toAltName(dbName string, tableName string) string {
	targetDB := c.config.Database()
	if targetDB == "" {
		targetDB = dbName
	}
	if altName, ok := c.altNames[tableName]; ok {
		tableName = altName
	}
	return fmt.Sprintf("`%s`.`%s`", targetDB, tableName)
}

func (c *HTTPTarget) AsyncPush(input base.EventBatch) chan error {
	switch batch := input.(type) {
	case *HTTPEventsBatch:
		st := time.Now()
		table, err := batch.Part.ToOldTableDescription()
		if err != nil {
			return util.MakeChanWithError(xerrors.Errorf("unable to construct table description: %w", err))
		}
		columnNames := batch.ColumnNames()
		escapedColumnNames := make([]string, len(columnNames))
		for i, columnName := range columnNames {
			escapedColumnNames[i] = fmt.Sprintf("`%s`", columnName)
		}

		blob := []byte(fmt.Sprintf(
			"INSERT INTO %s (%s) %s FORMAT %s\n",
			c.toAltName(table.Schema, table.Name),
			strings.Join(escapedColumnNames, ","),
			c.config.InsertSettings().AsQueryPart(),
			batch.Format,
		))
		blob = append(blob, batch.Data...)
		err = backoff.RetryNotify(
			func() error {
				st := time.Now()
				host := c.HostByPart(batch.Part)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				if err := c.client.Exec(ctx, c.logger, host, string(blob)); err != nil {
					if subMatch := syntaxErrorRegexp.FindStringSubmatch(err.Error()); subMatch != nil {
						errorRowNumber, _ := strconv.Atoi(subMatch[1])
						rowBuffer := strings.Builder{}
						csvSplitter := util.NewLineSplitter(bytes.NewReader(batch.Data), &rowBuffer)
						for i := 1; ; i++ {
							rowBuffer.Reset()
							if err := csvSplitter.ConsumeRow(); err != nil {
								c.logger.Warnf("invalid CSV input at line %d: %s", i, err.Error())
								break
							}
							if i >= errorRowNumber {
								c.logger.Errorf("errored at row %d: %s; %dth row: %s", errorRowNumber, err.Error(), i, rowBuffer.String())
								break
							}
						}
					}
					return err
				}
				c.logger.Infof("%v blob %v uploaded to %v in: %v", batch.Part.FullName(), format.SizeInt(len(blob)), host.String(), time.Since(st))
				return nil
			},
			backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 10),
			util.BackoffLogger(
				c.logger,
				fmt.Sprintf("push %v data", format.SizeInt(len(batch.Data))),
			),
		)
		if err != nil {
			return util.MakeChanWithError(xerrors.Errorf("unable to push http batch: %v: %w", batch.Part.FullName(), err))
		}
		c.wrapperMetrics.Timer.RecordDuration(time.Since(st))
		c.wrapperMetrics.ChangeItemsPushed.Add(1)
		c.wrapperMetrics.RowEventsPushed.Add(int64(batch.RowCount))
		c.metrics.Table(batch.Part.Name(), "rows", batch.RowCount)
		return util.MakeChanWithError(nil)
	case *schema.DDLBatch:
		for _, ddl := range batch.DDLs {
			err := c.execDDL(func(distributed bool) error {
				query, err := c.adjustDDLToTarget(ddl, distributed)
				if err != nil {
					return xerrors.Errorf("unable to adjust DDL: %w", err)
				}
				if err := c.client.Exec(context.Background(), c.logger, c.HostByPart(nil), query); err != nil {
					return xerrors.Errorf("unable to exec DDL(%v): %w", query, err)
				}
				c.logger.Infof("ddl completed: %v", query)
				return nil
			})
			if err != nil {
				return util.MakeChanWithError(err)
			}
		}
		return util.MakeChanWithError(nil)
	case base.EventBatch:
		for batch.Next() {
			ev, err := batch.Event()
			if err != nil {
				return util.MakeChanWithError(xerrors.Errorf("unable to extract event: %w", err))
			}
			var ddl string
			switch event := ev.(type) {
			case events.CleanupEvent:
				switch c.config.Cleanup() {
				case dp_model.DisabledCleanup:
					continue
				case dp_model.Truncate:
					ddl = "TRUNCATE TABLE IF EXISTS %s"
				case dp_model.Drop:
					ddl = "DROP TABLE IF EXISTS %s NO DELAY"
				}

				err := c.execDDL(func(distributed bool) error {
					q := fmt.Sprintf(ddl, c.tableReferenceForDDL(c.toAltName(event.Namespace, event.Name), distributed))
					return c.client.Exec(context.Background(), c.logger, c.HostByPart(nil), q)
				})
				if err != nil {
					return util.MakeChanWithError(xerrors.Errorf("unable to %s: %s: %w", c.config.Cleanup(), event, err))
				}
			case events.TableLoadEvent:
				// not needed for now
			default:
				return util.MakeChanWithError(xerrors.Errorf("unexpected event type: %T", ev))
			}
		}
		return util.MakeChanWithError(nil)
	default:
		return util.MakeChanWithError(xerrors.Errorf("unexpected input type: %T", input))
	}
}

func (c *HTTPTarget) Close() error {
	return nil
}

func (c *HTTPTarget) adjustDDLToTarget(ddl schema.TableDDL, distributed bool) (string, error) {
	ddlChangeItem := ddl.ToChangeItem()
	sqlDDL := ddl.SQL()
	sqlDDL = schema.SetTargetDatabase(sqlDDL, ddl.TableID().Namespace, c.config.Database())
	sqlDDL = schema.SetAltName(sqlDDL, c.config.Database(), MakeAltNames(c.config))
	sqlDDL = schema.SetIfNotExists(sqlDDL)
	switch ddlChangeItem.Kind {
	case abstract.ChCreateTableDistributedKind:
		sqlDDL = schema.ReplaceCluster(sqlDDL, c.cluster.Name())
	}
	if distributed {
		if ddlChangeItem.Kind == abstract.ChCreateTableKind {
			sqlDDL = schema.MakeDistributedDDL(sqlDDL, c.cluster.Name())
		}
		engine := ddlChangeItem.ColumnValues[1].(string)
		if engine == string(schema.MaterializedView) {
			underlying, _, err := schema.ParseMergeTreeFamilyEngine(sqlDDL)
			if err != nil {
				c.logger.Warnf("unsupported engine in materialized view %v.%v: %v", ddl.TableID().Namespace, ddl.TableID().Name, err)
				return sqlDDL, nil
			}
			engine = string(underlying.Type)
		}

		if schema.IsSharedEngineType(engine) {
			replicated, err := schema.GetReplicatedFromSharedEngineType(engine)
			if err != nil {
				return "", xerrors.Errorf("unable to get replicated from shared engine: %w", err)
			}

			sqlDDL = strings.Replace(sqlDDL, engine, replicated, 1)
		}

		if schema.IsMergeTreeFamily(engine) && !schema.IsReplicatedEngineType(engine) && !schema.IsSharedEngineType(engine) {
			if query, err := schema.SetReplicatedEngine(sqlDDL, engine, ddl.TableID().Namespace, ddl.TableID().Name); err != nil {
				return query, xerrors.Errorf("unable to set replicated table engine: %w", err)
			} else {
				sqlDDL = query
			}
		}
	} // maybe we also should decrease engine
	return sqlDDL, nil
}

func (c *HTTPTarget) HostByPart(part *TablePartA2) *clickhouse.Host {
	host := new(clickhouse.Host)
	if c.config.Host() != nil {
		host = c.config.Host()
	}
	if host.HostName() == "" && len(c.config.AltHosts()) > 0 {
		randomIndex := rand.Intn(len(c.config.AltHosts()))
		host = c.config.AltHosts()[randomIndex]
	}

	// Choose random host of first shard for cluster DDL
	if host.HostName() == "" && part == nil && len(c.config.Shards()) > 0 {
		for shardName, hosts := range c.config.Shards() {
			idx := rand.Intn(len(hosts))
			host = hosts[idx]
			c.logger.Debugf("choose random host %s of shard %s for DDL query", host.String(), shardName)
			return host
		}
	}

	if part != nil {
		if part.ShardCount == len(c.cluster.Shards) {
			randomIndex := rand.Intn(len(c.cluster.Shards[part.ShardNum]))
			host = c.cluster.Shards[part.ShardNum][randomIndex]
		} else {
			targetShardNum := part.ShardNum%len(c.cluster.Shards) + 1
			shardHosts := c.cluster.Shards[targetShardNum]
			randomIndex := rand.Intn(len(shardHosts))
			host = shardHosts[randomIndex]
			c.logger.Debugf("choose host: %v from %v, source shard: %v, target shard: %v", host.String(), HostsToString(shardHosts), part.ShardNum, targetShardNum)
		}
	}
	return host
}

func (c *HTTPTarget) tableReferenceForDDL(name string, distributed bool) string {
	cluster := ""
	if distributed {
		cluster = fmt.Sprintf(" ON CLUSTER `%s`", c.cluster.Name())
	}
	return fmt.Sprintf("%s%s", name, cluster)
}

func (c *HTTPTarget) resolveCluster() error {
	t, err := topology.ResolveTopology(c.config, c.logger)
	if err != nil {
		return xerrors.Errorf("error resolving cluster topology: %w", err)
	}

	shardMap := make(topology.ShardHostMap)
	shardNames := maps.Keys(c.config.Shards())
	sort.Strings(shardNames)
	for i, shardName := range shardNames {
		shardMap[i+1] = c.config.Shards()[shardName] // shard indexing start with 1
	}

	c.cluster = &topology.Cluster{
		Topology: *t,
		Shards:   shardMap,
	}
	return nil
}

func (c *HTTPTarget) execDDL(executor func(distributed bool) error) error {
	c.distributedDDLMu.Lock()

	if c.distributedDDLEnabled == nil && (c.cluster == nil || c.cluster.Name() == "") {
		if !c.cluster.SingleNode() {
			return xerrors.Errorf("resolved empty cluster or cluster name for non-single-node cluster")
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

	if !errors.IsDistributedDDLError(err) {
		return xerrors.Errorf("error executing DDL: %w", err)
	}
	c.logger.Error("Got distributed DDL error", log.Error(err))

	if !c.cluster.SingleNode() {
		c.logger.Error("cluster is not single node and distributed DDL is not available")
		return errors.ForbiddenDistributedDDLError
	}

	if err := executor(false); err != nil {
		return xerrors.Errorf("error executing DDL: %w", err)
	}
	c.logger.Warn("disabling distributed DDL for cluster")
	c.distributedDDLEnabled = ptr.Bool(false)
	return nil
}

func newHTTPTargetImpl(transfer *dp_model.Transfer, config model.ChSinkParams, mtrc metrics.Registry, logger log.Logger) (*HTTPTarget, error) {
	client, err := httpclient.NewHTTPClientImpl(config)
	if err != nil {
		return nil, xerrors.Errorf("error creating CH HTTP client: %w", err)
	}

	target := &HTTPTarget{
		client:         client,
		config:         config,
		logger:         logger,
		cluster:        nil,
		altNames:       MakeAltNames(config),
		metrics:        stats.NewSinkerStats(mtrc),
		wrapperMetrics: stats.NewWrapperStats(mtrc),

		distributedDDLEnabled: nil,
		distributedDDLMu:      sync.Mutex{},
	}

	if err := target.resolveCluster(); err != nil {
		return nil, xerrors.Errorf("error resolving cluster topology: %w", err)
	}

	return target, nil
}

func NewHTTPTarget(transfer *dp_model.Transfer, mtrc metrics.Registry, logger log.Logger) (*HTTPTarget, error) {
	dst, ok := transfer.Dst.(*model.ChDestination)
	if !ok {
		panic("expected ClickHouse destination in ClickHouse sink constructor")
	}

	sinkParams, err := dst.ToSinkParams(transfer)
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve sink params: %w", err)
	}

	return newHTTPTargetImpl(transfer, sinkParams, mtrc, logger)
}
