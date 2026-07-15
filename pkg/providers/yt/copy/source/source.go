package source

import (
	"context"
	"strings"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract2"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_copy_events "github.com/transferia/transferia/pkg/providers/yt/copy/events"
	"github.com/transferia/transferia/pkg/providers/yt/cypressmeta"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

type source struct {
	cfg               provider_yt.YtSourceModel
	yt                yt.Client
	nodes             cypressmeta.YtNodes
	snapshotID        string
	snapshotIsRunning bool
	snapshotEvtBatch  *yt_copy_events.EventBatch
	skipLinkFollowing bool
	logger            log.Logger
	metrics           core_metrics.Registry
}

// To verify providers contract implementation
var (
	_ abstract2.SnapshotProvider = (*source)(nil)
)

func (s *source) Init() error {
	return nil
}

func (s *source) Ping() error {
	return nil
}

func (s *source) Close() error {
	s.yt.Stop()
	return nil
}

func (s *source) BeginSnapshot() error {
	s.logger.Debug("Begining snapshot")
	ctx := context.Background()
	var err error
	if s.nodes, err = cypressmeta.ListNodes(ctx, s.yt, s.cfg.GetCluster(), s.cfg.GetPaths(), []yt.NodeType{yt.NodeTable, yt.NodeFile}, s.skipLinkFollowing, s.logger); err != nil {
		return xerrors.Errorf("error getting list of nodes: %w", err)
	}
	s.logger.Infof("Got %d nodes to copy", len(s.nodes))
	s.snapshotID = strings.Join(s.cfg.GetPaths(), ";")
	s.logger.Debugf("SnapshotID is %s", s.snapshotID)
	return nil
}

func (s *source) EndSnapshot() error {
	s.logger.Debug("Ending snapshot")
	s.snapshotID = ""
	return nil
}

func (s *source) DataObjects(filter abstract2.DataObjectFilter) (abstract2.DataObjects, error) {
	return newDataObjects(s.snapshotID), nil
}

func (s *source) TableSchema(part abstract2.DataObjectPart) (*abstract.TableSchema, error) {
	return nil, nil // this is special homo-copy-source
}

func (s *source) CreateSnapshotSource(part abstract2.DataObjectPart) (abstract2.ProgressableEventSource, error) {
	s.logger.Debugf("Creating snapshot source for %s", s.snapshotID)
	if part.FullName() != s.snapshotID {
		return nil, xerrors.Errorf("part name %s doesn't match current snapshot tx id %s", part.FullName(), s.snapshotID)
	}
	return s, nil
}

func (s *source) ResolveOldTableDescriptionToDataPart(tableDesc abstract.TableDescription) (abstract2.DataObjectPart, error) {
	return nil, xerrors.New("legacy table desc is not supported")
}

func (s *source) DataObjectsToTableParts(filter abstract2.DataObjectFilter) ([]abstract.TableDescription, error) {
	objects, err := s.DataObjects(filter)
	if err != nil {
		return nil, xerrors.Errorf("Can't get data objects: %w", err)
	}

	tableDescriptions, err := abstract2.DataObjectsToTableParts(objects, filter)
	if err != nil {
		return nil, xerrors.Errorf("Can't convert data objects to table descriptions: %w", err)
	}

	return tableDescriptions, nil
}

func (s *source) TablePartToDataObjectPart(tableDescription *abstract.TableDescription) (abstract2.DataObjectPart, error) {
	if tableDescription == nil {
		return nil, xerrors.New("table description is nil")
	}

	return dataObjectPart(tableDescription.Name), nil
}

func (s *source) Running() bool {
	return s.snapshotIsRunning
}

func (s *source) Start(ctx context.Context, target abstract2.EventTarget) error {
	s.logger.Debugf("Starting snapshot source for %s", s.snapshotID)
	defer func() {
		s.snapshotIsRunning = false
	}()
	s.snapshotIsRunning = true
	s.snapshotEvtBatch = yt_copy_events.NewEventBatch(s.nodes)
	return <-target.AsyncPush(s.snapshotEvtBatch)
}

func (s *source) Stop() error {
	s.snapshotIsRunning = false
	return nil
}

func (s *source) Progress() (abstract2.EventSourceProgress, error) {
	if s.snapshotEvtBatch == nil {
		return abstract2.NewDefaultEventSourceProgress(false, uint64(0), uint64(len(s.nodes))), nil
	}
	return s.snapshotEvtBatch.Progress(), nil
}

func NewSource(logger log.Logger, metrics core_metrics.Registry, cfg provider_yt.YtSourceModel, skipLinkFollowing bool, transferID string) (*source, error) {
	y, err := yt_client.FromConnParams(cfg, logger)
	if err != nil {
		return nil, xerrors.Errorf("error creating ytrpc client: %w", err)
	}
	return &source{
		cfg:               cfg,
		yt:                y,
		nodes:             nil,
		snapshotID:        "",
		snapshotIsRunning: false,
		snapshotEvtBatch:  nil,
		skipLinkFollowing: skipLinkFollowing,
		logger:            logger,
		metrics:           metrics,
	}, nil
}
