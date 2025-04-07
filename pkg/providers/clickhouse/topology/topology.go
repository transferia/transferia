package topology

import (
	"context"
	"database/sql"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	conn2 "github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/exp/maps"
)

var ErrNoCluster = xerrors.New("no clusters found in system.clusters table")

type Topology struct {
	clusterName string
	singleNode  bool
}

func (t *Topology) ClusterName() string {
	return t.clusterName
}

func (t *Topology) SingleNode() bool {
	return t.singleNode
}

func resolveClusterName(ctx context.Context, db *sql.DB, cfg model.ChSinkServerParams) (string, error) {
	// case 1: forced cluster name via config (it could be MDB shard group)
	if cfgName := cfg.ChClusterName(); cfgName != "" {
		return cfgName, nil
	}

	// case 2: resolve default cluster for MDB
	if cfg.MdbClusterID() != "" {
		var substitution string
		if err := db.QueryRowContext(ctx, `select substitution from system.macros where macro = 'cluster';`).Scan(&substitution); err != nil {
			return "", xerrors.Errorf("unable to resolve cluster macro: %w", err)
		}
		return substitution, nil
	}

	// case 3: for on-prem try to take first cluster from system table
	// TODO: this legacy behaviour probably should be replaced by making cluster name setting available in UI
	var name string
	if err := db.QueryRowContext(ctx, `select cluster from system.clusters limit 1;`).Scan(&name); err != nil {
		if xerrors.Is(err, sql.ErrNoRows) {
			return "", ErrNoCluster
		}
		return "", xerrors.Errorf("error listing clusters: %w", err)
	}
	return name, nil
}

func IsSingleNode(shards map[string][]string) bool {
	return len(shards) == 1 && len(maps.Values(shards)[0]) == 1
}

func validateShards(shards map[string][]string) error {
	if len(shards) < 1 {
		return xerrors.New("empty shards config")
	}
	for name, hosts := range shards {
		if len(hosts) < 1 {
			return xerrors.Errorf("empty host list for shard %s", name)
		}
	}
	return nil
}

func ResolveTopology(params model.ChSinkParams, lgr log.Logger) (*Topology, error) {
	shards := params.Shards()
	if err := validateShards(shards); err != nil {
		return nil, xerrors.Errorf("invalid shards config: %w", err)
	}

	singleNode := IsSingleNode(shards)
	var allHosts, remainHosts []string
	for _, hosts := range shards {
		allHosts = append(allHosts, hosts...)
	}

	// Cyclic iterate over all cluster hosts in random order
	clusterName, err := backoff.RetryNotifyWithData(func() (string, error) {
		if len(remainHosts) == 0 {
			remainHosts = make([]string, len(allHosts))
			copy(remainHosts, allHosts)
			remainHosts = yslices.Shuffle(remainHosts, nil)
		}
		host := remainHosts[0]
		remainHosts = remainHosts[1:]

		lgr.Infof("Trying to resolve cluster name from host %s", host)
		conn, err := conn2.ConnectNative(host, params)
		if err != nil {
			return "", xerrors.Errorf("error connecting to clickhouse host %s: %w", host, err)
		}
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		clusterName, err := resolveClusterName(ctx, conn, params)
		if xerrors.Is(err, ErrNoCluster) {
			//nolint:descriptiveerrors
			return "", backoff.Permanent(err)
		}
		//nolint:descriptiveerrors
		return clusterName, err
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 10), util.BackoffLogger(lgr, "failed to resolve cluster topology"))

	if err != nil {
		if singleNode {
			lgr.Warn("Error getting cluster name from single node cluster", log.Error(err))
		} else {
			return nil, xerrors.Errorf("error getting cluster name: %w", err)
		}
	}

	return NewTopology(clusterName, singleNode), nil
}

func NewTopology(clusterName string, singleNode bool) *Topology {
	return &Topology{
		clusterName: clusterName,
		singleNode:  singleNode,
	}
}
