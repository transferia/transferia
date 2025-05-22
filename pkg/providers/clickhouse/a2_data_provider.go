package clickhouse

import (
	"context"
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type DataProvider struct {
	logger     log.Logger
	registry   metrics.Registry
	config     *model.ChSource
	transferID string
	shards     map[string][]*clickhouse.Host
	storage    ClickhouseStorage
}

// To verify providers contract implementation
var (
	_ base.SnapshotProvider = (*DataProvider)(nil)
)

func (c *DataProvider) TableSchema(part base.DataObjectPart) (*abstract.TableSchema, error) {
	desc, err := part.ToOldTableDescription()
	if err != nil {
		return nil, xerrors.Errorf("unable to load part: %w", err)
	}
	return c.storage.TableSchema(context.Background(), desc.ID())
}

func (c *DataProvider) BeginSnapshot() error {
	return nil
}

func (c *DataProvider) DataObjects(filter base.DataObjectFilter) (base.DataObjects, error) {
	return NewClusterTables(c.storage, c.config, filter)
}

func (c *DataProvider) CreateSnapshotSource(part base.DataObjectPart) (base.ProgressableEventSource, error) {
	st, ok := part.(*TablePartA2)
	if !ok {
		return nil, xerrors.Errorf("unexpected part type %T, expected: TableShard", part)
	}
	td, err := st.ToOldTableDescription()
	if err != nil {
		return nil, xerrors.Errorf("unable to build description: %w", err)
	}
	cols, selectQuery, countQuery, err := c.storage.BuildTableQuery(*td)
	if err != nil {
		return nil, xerrors.Errorf("unable to build table query: %v: %w", td.Fqtn(), err)
	}
	c.logger.Infof("create snapshot source for: %v: %v", st.Shard, part.FullName())
	hosts := c.shards[st.Shard]
	if len(c.shards) == 1 {
		for shard := range c.shards {
			hosts = c.shards[shard]
		}
	}
	if len(hosts) == 0 {
		return nil, xerrors.Errorf("unable to found hosts for shard: %v", st.Shard)
	}
	httpSrc, err := NewHTTPSource(
		c.logger,
		selectQuery,
		countQuery,
		cols,
		hosts,
		c.config,
		st,
		stats.NewSourceStats(c.registry),
	)
	if err != nil {
		return nil, xerrors.Errorf("error creating HTTP source: %w", err)
	}
	return NewSourcesChain(
		c.logger,
		schema.NewDDLSource(
			c.logger,
			part,
			c.storage,
		),
		httpSrc,
	), nil
}

func (c *DataProvider) EndSnapshot() error {
	return nil
}

func (c *DataProvider) ResolveOldTableDescriptionToDataPart(tableDesc abstract.TableDescription) (base.DataObjectPart, error) {
	return nil, xerrors.New("not implemented")
}

func (c *DataProvider) Init() error {
	return nil
}

func (c *DataProvider) Ping() error {
	return c.storage.Ping()
}

func (c *DataProvider) Close() error {
	c.storage.Close()
	return nil
}

func (c *DataProvider) DataObjectsToTableParts(filter base.DataObjectFilter) ([]abstract.TableDescription, error) {
	objects, err := c.DataObjects(filter)
	if err != nil {
		return nil, xerrors.Errorf("Can't get data objects: %w", err)
	}

	tableDescriptions, err := base.DataObjectsToTableParts(objects, filter)
	if err != nil {
		return nil, xerrors.Errorf("Can't convert data objects to table descriptions: %w", err)
	}

	return tableDescriptions, nil
}

func (c *DataProvider) TablePartToDataObjectPart(tableDescription *abstract.TableDescription) (base.DataObjectPart, error) {
	var part TablePartA2
	if err := json.Unmarshal([]byte(tableDescription.Filter), &part); err != nil {
		return nil, xerrors.Errorf("Can't deserialize table part: %w", err)
	}
	return &part, nil
}

func NewClickhouseProvider(logger log.Logger, registry metrics.Registry, config *model.ChSource, transfer *dp_model.Transfer) (base.SnapshotProvider, error) {
	sinkParams, err := config.ToSinkParams()
	if err != nil {
		return nil, xerrors.Errorf("unable to get sink params: %w", err)
	}
	shards := sinkParams.Shards()
	storageParams, err := config.ToStorageParams()
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve storage params")
	}
	logger.Infof("init clickhouse provider: %v", shards)
	storage, err := NewStorage(storageParams, transfer, WithHomo(), WithTableFilter(config))
	if err != nil {
		return nil, err
	}
	return &DataProvider{
		logger:     logger,
		registry:   registry,
		config:     config,
		transferID: transfer.ID,
		shards:     shards,
		storage:    storage,
	}, nil
}
