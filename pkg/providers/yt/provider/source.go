//go:build !disable_yt_provider

package provider

import (
	"context"

	"github.com/gofrs/uuid"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/providers/yt/provider/dataobjects"
	"github.com/transferia/transferia/pkg/providers/yt/provider/schema"
	"github.com/transferia/transferia/pkg/providers/yt/tablemeta"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

type source struct {
	cfg     *yt2.YtSource
	yt      yt.Client
	tx      yt.Tx
	txID    yt.TxID
	logger  log.Logger
	tables  tablemeta.YtTables
	metrics *stats.SourceStats
}

// To verify providers contract implementation
var (
	_ base.SnapshotProvider = (*source)(nil)
)

func NewSource(logger log.Logger, registry metrics.Registry, cfg *yt2.YtSource) (*source, error) {
	ytc, err := ytclient.FromConnParams(cfg.ConnParams(), logger)
	if err != nil {
		return nil, xerrors.Errorf("unable to create yt client: %w", err)
	}
	return &source{
		cfg:     cfg,
		yt:      ytc,
		tx:      nil,
		txID:    yt.TxID(uuid.Nil),
		logger:  logger,
		tables:  nil,
		metrics: stats.NewSourceStats(registry),
	}, nil
}

func (s *source) Init() error {
	return nil
}

func (s *source) Ping() error {
	return nil
}

func (s *source) Close() error {
	return nil
}

func (s *source) BeginSnapshot() error {
	tx, err := s.yt.BeginTx(context.Background(), nil)
	if err != nil {
		return xerrors.Errorf("error starting snapshot TX: %w", err)
	}
	s.tx = tx
	s.txID = tx.ID()
	return nil
}

func (s *source) DataObjects(filter base.DataObjectFilter) (base.DataObjects, error) {
	return s.dataObjectsCore(filter), nil
}

func (s *source) dataObjectsCore(filter base.DataObjectFilter) *dataobjects.YTDataObjects {
	return dataobjects.NewDataObjects(s.cfg, s.tx, s.logger, filter)
}

func (s *source) TableSchema(part base.DataObjectPart) (*abstract.TableSchema, error) {
	p, ok := part.(*dataobjects.Part)
	if !ok {
		return nil, xerrors.Errorf("part %T is not yt dataobject part: %s", part, part.FullName())
	}
	yttable, err := schema.Load(context.Background(), s.yt, s.tx.ID(), p.NodeID(), p.Name())
	if err != nil {
		return nil, xerrors.Errorf("unable to load yt schema: %w", err)
	}
	return yttable.ToOldTable()
}

func (s *source) CreateSnapshotSource(part base.DataObjectPart) (base.ProgressableEventSource, error) {
	p, ok := part.(*dataobjects.Part)
	if !ok {
		return nil, xerrors.Errorf("part %T is not yt dataobject part: %s", part, part.FullName())
	}
	return NewSnapshotSource(s.cfg, s.yt, p, s.logger, s.metrics), nil
}

func (s *source) EndSnapshot() error {
	// Since the only goal of TX is to hold snapshot lock and no data modification should happen,
	// it is safe to ignore any errors, TX may be already aborted or will be aborted by YT after transfer ends
	if err := s.tx.Abort(); err != nil {
		s.logger.Warn("Error aborting YT snapshot TX", log.Error(err))
	}
	return nil
}

func (s *source) ResolveOldTableDescriptionToDataPart(tableDesc abstract.TableDescription) (base.DataObjectPart, error) {
	return nil, xerrors.New("legacy is not supported")
}

func (s *source) DataObjectsToTableParts(filter base.DataObjectFilter) ([]abstract.TableDescription, error) {
	return s.dataObjectsCore(filter).ToTableParts()
}

func (s *source) TablePartToDataObjectPart(tableDescription *abstract.TableDescription) (base.DataObjectPart, error) {
	key, err := dataobjects.ParsePartKey(string(tableDescription.Filter))
	if err != nil {
		return nil, xerrors.Errorf("Can't parse part key: %w", err)
	}
	return dataobjects.NewPart(key.Table, key.NodeID, key.Range(), s.txID), nil
}

func (s *source) ShardingContext() ([]byte, error) {
	txID, err := yson.MarshalFormat(s.txID, yson.FormatText)
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal TxID: %w", err)
	}
	return txID, nil
}

func (s *source) SetShardingContext(shardedState []byte) error {
	if err := yson.Unmarshal(shardedState, &s.txID); err != nil {
		return xerrors.Errorf("unable to unmarhsal TxID: %w", err)
	}
	return nil
}
