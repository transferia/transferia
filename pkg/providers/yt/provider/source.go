package provider

import (
	"context"
	"time"

	gofrs_uuid "github.com/gofrs/uuid"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract2"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/cypressmeta"
	"github.com/transferia/transferia/pkg/providers/yt/provider/dataobjects"
	yt_provider_schema "github.com/transferia/transferia/pkg/providers/yt/provider/schema"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

type source struct {
	cfg          provider_yt.YtSourceModel
	yt           yt.Client
	tx           yt.Tx
	txID         yt.TxID
	logger       log.Logger
	tables       cypressmeta.YtNodes
	metrics      *stats.SourceStats
	columnFilter map[yt.NodeID][]string
}

// To verify providers contract implementation
var (
	_ abstract2.SnapshotProvider = (*source)(nil)

	mainTxTimeout = yson.Duration(10 * time.Minute)
)

func NewSource(logger log.Logger, registry core_metrics.Registry, cfg provider_yt.YtSourceModel, include []string) (*source, error) {
	ytc, err := yt_client.FromConnParams(cfg, logger)
	if err != nil {
		return nil, xerrors.Errorf("unable to create yt client: %w", err)
	}

	columnFilter, err := buildColumnFilter(ytc, include)
	if err != nil {
		return nil, xerrors.Errorf("failed to build column filter: %w", err)
	}

	return &source{
		cfg:          cfg,
		yt:           ytc,
		tx:           nil,
		txID:         yt.TxID(gofrs_uuid.Nil),
		logger:       logger,
		tables:       nil,
		metrics:      stats.NewSourceStats(registry),
		columnFilter: columnFilter,
	}, nil
}

func buildColumnFilter(client yt.Client, include []string) (map[yt.NodeID][]string, error) {
	res := make(map[yt.NodeID][]string)

	for _, object := range include {
		yp, err := ypath.Parse(object)
		if err != nil {
			return nil, xerrors.Errorf("cannot parse input ypath %s: %w", object, err)
		}
		var nodeID yt.NodeID
		if err := client.GetNode(context.Background(), yp.YPath().Attr("id"), &nodeID, nil); err != nil {
			return nil, xerrors.Errorf("cannot get node id for table %s: %w", object, err)
		}
		res[nodeID] = yp.Columns
	}
	return res, nil
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
	tx, err := s.yt.BeginTx(context.Background(), &yt.StartTxOptions{Timeout: &mainTxTimeout})
	if err != nil {
		return xerrors.Errorf("error starting snapshot TX: %w", err)
	}
	s.tx = tx
	s.txID = tx.ID()
	return nil
}

func (s *source) DataObjects(filter abstract2.DataObjectFilter) (abstract2.DataObjects, error) {
	return s.dataObjectsCore(filter), nil
}

func (s *source) dataObjectsCore(filter abstract2.DataObjectFilter) *dataobjects.YTDataObjects {
	return dataobjects.NewDataObjects(s.cfg, s.tx, s.logger, filter)
}

func (s *source) TableSchema(part abstract2.DataObjectPart) (*abstract.TableSchema, error) {
	p, ok := part.(*dataobjects.Part)
	if !ok {
		return nil, xerrors.Errorf("part %T is not yt dataobject part: %s", part, part.FullName())
	}
	yttable, err := yt_provider_schema.Load(context.Background(), s.yt, s.tx.ID(), p.NodeID(), p.Name(), s.columnFilter[p.NodeID()])
	if err != nil {
		return nil, xerrors.Errorf("unable to load yt schema: %w", err)
	}
	return yttable.ToOldTable()
}

func (s *source) CreateSnapshotSource(part abstract2.DataObjectPart) (abstract2.ProgressableEventSource, error) {
	p, ok := part.(*dataobjects.Part)
	if !ok {
		return nil, xerrors.Errorf("part %T is not yt dataobject part: %s", part, part.FullName())
	}
	return NewSnapshotSource(s.cfg, s.yt, p, s.logger, s.metrics, s.columnFilter[p.NodeID()]), nil
}

func (s *source) EndSnapshot() error {
	// Since the only goal of TX is to hold snapshot lock and no data modification should happen,
	// it is safe to ignore any errors, TX may be already aborted or will be aborted by YT after transfer ends
	if err := s.tx.Abort(); err != nil {
		s.logger.Warn("Error aborting YT snapshot TX", log.Error(err))
	}
	return nil
}

func (s *source) ResolveOldTableDescriptionToDataPart(tableDesc abstract.TableDescription) (abstract2.DataObjectPart, error) {
	return nil, xerrors.New("legacy is not supported")
}

func (s *source) DataObjectsToTableParts(filter abstract2.DataObjectFilter) ([]abstract.TableDescription, error) {
	return s.dataObjectsCore(filter).ToTableParts()
}

func (s *source) TablePartToDataObjectPart(tableDescription *abstract.TableDescription) (abstract2.DataObjectPart, error) {
	key, err := dataobjects.ParsePartKey(string(tableDescription.Filter))
	if err != nil {
		return nil, xerrors.Errorf("Can't parse part key: %w", err)
	}
	return dataobjects.NewPart(key.Table, key.NodeID, key.Range(), s.txID), nil
}

type SnapshotState struct {
	TxID         yt.TxID                `yson:"tx_id"`
	ColumnFilter map[yt.NodeID][]string `yson:"column_filter"`
}

func (s *source) ShardingContext() ([]byte, error) {
	ysonctx, err := yson.Marshal(&SnapshotState{
		TxID:         s.txID,
		ColumnFilter: s.columnFilter,
	})
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal yt config: %w", err)
	}
	return ysonctx, nil
}

func (s *source) SetShardingContext(shardedState []byte) error {
	res := new(SnapshotState)
	if err := yson.Unmarshal(shardedState, res); err != nil {
		return xerrors.Errorf("unable to unmarshal sharding state for yt: %w", err)
	}
	s.txID = res.TxID
	s.columnFilter = res.ColumnFilter
	return nil
}
