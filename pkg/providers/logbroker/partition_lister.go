package logbroker

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	topicapisource "github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type lbPartitionLister struct {
	listers map[string]abstract.PartitionLister
}

func (l *lbPartitionLister) ListPartitions() ([]abstract.Partition, error) {
	var result []abstract.Partition
	for cluster, lister := range l.listers {
		partitions, err := lister.ListPartitions()
		if err != nil {
			return nil, xerrors.Errorf("failed to list partitions for cluster %s: %w", cluster, err)
		}

		for _, partition := range partitions {
			partition.Cluster = cluster
			result = append(result, partition)
		}
	}

	return result, nil
}

func (l *lbPartitionLister) Close() {
	for _, lister := range l.listers {
		lister.Close()
	}
}

func NewPartitionLister(cfg *LfSource, logger log.Logger) (abstract.PartitionLister, error) {
	instances := []LogbrokerInstance{cfg.Instance}
	if cfg.Cluster != "" {
		if clusterInstances, ok := KnownClusters[cfg.Cluster]; ok {
			instances = clusterInstances
		}
	}

	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()

	listers := make(map[string]abstract.PartitionLister, len(instances))
	for _, instance := range instances {
		instanceCfgCopy := *cfg
		instanceCfgCopy.Instance = instance
		topicSourceCfg := instanceCfgCopy.buildTopicSourceConfig()

		lister, err := topicapisource.NewPartitionLister(topicSourceCfg, logger)
		if err != nil {
			return nil, xerrors.Errorf("unable to create partition lister for endpoint %s: %w", topicSourceCfg.Connection.Endpoint, err)
		}

		listers[string(instance)] = lister
		rollbacks.Add(lister.Close)
	}

	rollbacks.Cancel()

	return &lbPartitionLister{
		listers: listers,
	}, nil
}
