//go:build !disable_yt_provider

package source

import (
	"context"
	"strings"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/providers/yt/copy/events"
	"github.com/transferia/transferia/pkg/providers/yt/tablemeta"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

type source struct {
	cfg               *yt2.YtSource
	yt                yt.Client
	tables            tablemeta.YtTables
	snapshotID        string
	snapshotIsRunning bool
	snapshotEvtBatch  *events.EventBatch
	logger            log.Logger
	metrics           metrics.Registry
}

// To verify providers contract implementation
var (
	_ base.SnapshotProvider = (*source)(nil)
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
	if s.tables, err = tablemeta.ListTables(ctx, s.yt, s.cfg.Cluster, s.cfg.Paths, s.logger); err != nil {
		return xerrors.Errorf("error getting list of tables: %w", err)
	}
	s.logger.Infof("Got %d tables to copy", len(s.tables))
	s.snapshotID = strings.Join(s.cfg.Paths, ";")
	s.logger.Debugf("SnapshotID is %s", s.snapshotID)
	return nil
}

func (s *source) EndSnapshot() error {
	s.logger.Debug("Ending snapshot")
	s.snapshotID = ""
	return nil
}

func (s *source) DataObjects(filter base.DataObjectFilter) (base.DataObjects, error) {
	return newDataObjects(s.snapshotID), nil
}

func (s *source) TableSchema(part base.DataObjectPart) (*abstract.TableSchema, error) {
	return nil, nil // this is special homo-copy-source
}

func (s *source) CreateSnapshotSource(part base.DataObjectPart) (base.ProgressableEventSource, error) {
	s.logger.Debugf("Creating snapshot source for %s", s.snapshotID)
	if part.FullName() != s.snapshotID {
		return nil, xerrors.Errorf("part name %s doesn't match current snapshot tx id %s", part.FullName(), s.snapshotID)
	}
	return s, nil
}

func (s *source) ResolveOldTableDescriptionToDataPart(tableDesc abstract.TableDescription) (base.DataObjectPart, error) {
	return nil, xerrors.New("legacy table desc is not supported")
}

func (s *source) DataObjectsToTableParts(filter base.DataObjectFilter) ([]abstract.TableDescription, error) {
	objects, err := s.DataObjects(filter)
	if err != nil {
		return nil, xerrors.Errorf("Can't get data objects: %w", err)
	}

	tableDescriptions, err := base.DataObjectsToTableParts(objects, filter)
	if err != nil {
		return nil, xerrors.Errorf("Can't convert data objects to table descriptions: %w", err)
	}

	return tableDescriptions, nil
}

func (s *source) TablePartToDataObjectPart(tableDescription *abstract.TableDescription) (base.DataObjectPart, error) {
	if tableDescription == nil {
		return nil, xerrors.New("table description is nil")
	}

	return dataObjectPart(tableDescription.Name), nil
}

func (s *source) Running() bool {
	return s.snapshotIsRunning
}

func (s *source) Start(ctx context.Context, target base.EventTarget) error {
	s.logger.Debugf("Starting snapshot source for %s", s.snapshotID)
	defer func() {
		s.snapshotIsRunning = false
	}()
	s.snapshotIsRunning = true
	s.snapshotEvtBatch = events.NewEventBatch(s.tables)
	return <-target.AsyncPush(s.snapshotEvtBatch)
}

func (s *source) Stop() error {
	s.snapshotIsRunning = false
	return nil
}

func (s *source) Progress() (base.EventSourceProgress, error) {
	if s.snapshotEvtBatch == nil {
		return base.NewDefaultEventSourceProgress(false, uint64(0), uint64(len(s.tables))), nil
	}
	return s.snapshotEvtBatch.Progress(), nil
}

func NewSource(logger log.Logger, metrics metrics.Registry, cfg *yt2.YtSource, transferID string) (*source, error) {
	if cfg.Proxy == "" {
		cfg.Proxy = cfg.Cluster
	}

	y, err := ytclient.FromConnParams(cfg.ConnParams(), logger)
	if err != nil {
		return nil, xerrors.Errorf("error creating ytrpc client: %w", err)
	}
	return &source{
		cfg:               cfg,
		yt:                y,
		tables:            nil,
		snapshotID:        "",
		snapshotIsRunning: false,
		snapshotEvtBatch:  nil,
		logger:            logger,
		metrics:           metrics,
	}, nil
}
