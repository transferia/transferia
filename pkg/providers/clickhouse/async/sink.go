//go:build !disable_clickhouse_provider

package async

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/async/dao"
	"github.com/transferia/transferia/pkg/providers/clickhouse/async/model/db"
	"github.com/transferia/transferia/pkg/providers/clickhouse/async/model/parts"
	"github.com/transferia/transferia/pkg/providers/clickhouse/errors"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	sharding "github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	"github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type sink struct {
	cfg        model.ChSinkParams
	dao        *dao.DDLDAO
	parts      parts.PartMap
	resBacklog map[abstract.TablePartID][]chan error
	cl         ClusterClient
	transferID string
	lgr        log.Logger
	sharder    sharding.Sharder
	middleware MiddlewareApplier
}

var sinkClosedErr = xerrors.New("sink is closed")

func (s *sink) Close() error {
	var errs error
	for partID, part := range s.parts {
		s.lgr.Debugf("Sink: closing part %s", partID.PartID)
		if err := part.Close(); err != nil {
			errs = multierr.Append(errs, xerrors.Errorf("error closing part %s: %w", partID.PartID, err))
		}
		s.flushBacklog(partID, sinkClosedErr)
	}
	if err := s.middleware.Close(); err != nil {
		errs = multierr.Append(errs, xerrors.Errorf("error closing middleware pipeline: %w", err))
	}
	s.lgr.Debug("Sink: closing clusterClient")
	if err := s.cl.Close(); err != nil {
		errs = multierr.Append(errs, xerrors.Errorf("error closing CH cluster client: %w", err))
	}
	return errs
}

func (s *sink) AsyncPush(rawItems []abstract.ChangeItem) chan error {
	if len(rawItems) < 1 {
		return util.MakeChanWithError(nil)
	}

	items, err := s.middleware.Apply(rawItems)
	if err != nil {
		return util.MakeChanWithError(xerrors.Errorf("error applying middleware pipeline: %w", err))
	}

	head := items[0]
	if head.IsRowEvent() {
		return s.pushDataRows(items)
	}

	if len(items) > 1 {
		return util.MakeChanWithError(xerrors.New("only one non-row item in batch is supported now"))
	}

	switch head.Kind {
	case abstract.DoneTableLoad:
		return s.finishPart(head.TablePartID())
	case abstract.SynchronizeKind:
		return s.flushPart(head.TablePartID())
	default:
		err = s.processSyncEvents(head)
	}
	return util.MakeChanWithError(err)
}

func (s *sink) processSyncEvents(head abstract.ChangeItem) error {
	db := s.cfg.Database()
	tbl := head.Table
	pID := head.PartID
	switch head.Kind {
	case abstract.InitShardedTableLoad:
		// FIXME: create table on this action when InitTableLoad is fixed to always contain schema
		// return s.ensureTargetTable(head)
	case abstract.DoneShardedTableLoad:
		// TODO: support tmp policy for CH and move tmp to final dst here
	case abstract.InitTableLoad:
		if err := s.ensureTargetTable(head); err != nil {
			return xerrors.Errorf("unable to get or create target table %s.%s: %w", db, tbl, err)
		}
		if err := s.createPart(head.TablePartID()); err != nil {
			return xerrors.Errorf("error creating part %s for table %s.%s: %w", pID, db, tbl, err)
		}
	case abstract.TruncateTableKind, abstract.DropTableKind:
		if err := s.cleanup(head); err != nil {
			return xerrors.Errorf("cleanup failed on table %s.%s: %w", db, tbl, err)
		}
	default:
		return xerrors.Errorf("unexpected ChangeItem kind %s", head.Kind)
	}
	return nil
}

func (s *sink) cleanup(head abstract.ChangeItem) error {
	s.lgr.Info("Cleanup", log.String("kind", string(head.Kind)), log.String("table", head.Table))
	var fn func(db, tbl string) error
	switch head.Kind {
	case abstract.DropTableKind:
		fn = s.dao.DropTable
	case abstract.TruncateTableKind:
		fn = s.dao.TruncateTable
	default:
		return xerrors.Errorf("unknown kind of cleanup event %s", head.Kind)
	}
	return fn(s.cfg.Database(), head.Table)
}

func (s *sink) createPart(partID abstract.TablePartID) error {
	tableID := *abstract.NewTableID(s.cfg.Database(), partID.TableID.Name)
	if _, ok := s.parts[partID]; ok {
		return xerrors.Errorf("part %s of table %s already exists", partID.PartID, partID.Fqtn())
	}
	s.lgr.Infof("Adding part %s for table %s", partID.PartID, tableID.Name)
	prt := NewPart(partID, s.cfg.Database(), s.cl, s.dao, s.sharder, s.lgr, s.transferID)
	s.parts.Add(partID, prt)
	return nil
}

func (s *sink) addBacklog(partID abstract.TablePartID) chan error {
	resCh := make(chan error, 1)
	s.resBacklog[partID] = append(s.resBacklog[partID], resCh)
	return resCh
}

func (s *sink) flushBacklog(partID abstract.TablePartID, err error) {
	s.lgr.Info("Flushing push result backlog", log.Int("count", len(s.resBacklog[partID])), log.Error(err))
	for _, c := range s.resBacklog[partID] {
		c <- err
	}
	s.resBacklog[partID] = nil
}

func (s *sink) withBacklog(partID abstract.TablePartID, flushOnlyIfError bool, fn func() error) chan error {
	res := s.addBacklog(partID)
	if err := fn(); err != nil || !flushOnlyIfError {
		s.flushBacklog(partID, err)
	}
	return res
}

func (s *sink) mergePart(partID abstract.TablePartID) error {
	tbl := partID.TableID.Name
	if part := s.parts.Part(partID); part != nil {
		s.lgr.Infof("Going to commit part %s of table %s", partID.PartID, tbl)
		err := part.Commit()
		delete(s.parts, partID)
		return err
	} else {
		return xerrors.Errorf("not found part %s of table %s", partID.PartID, tbl)
	}
}

// flushPart commits all uncommited part data but allows to continue writing to the same part
func (s *sink) flushPart(partID abstract.TablePartID) chan error {
	return s.withBacklog(partID, false, func() error {
		s.lgr.Info("Need to synchronize, flushing part",
			log.String("part", partID.PartID), log.String("table", partID.Fqtn()))
		if err := s.mergePart(partID); err != nil {
			return xerrors.Errorf("error commiting data of the part %s of %s: %w", partID.PartID, partID.Fqtn(), err)
		}
		return s.createPart(partID)
	})
}

func (s *sink) finishPart(partID abstract.TablePartID) chan error {
	return s.withBacklog(partID, false, func() error {
		return s.mergePart(partID)
	})
}

func (s *sink) ensureTargetTable(item abstract.ChangeItem) error {
	s.lgr.Infof("Checking/creating target table")
	exists, err := s.dao.TableExists(s.cfg.Database(), item.Table)
	if err != nil {
		return xerrors.Errorf("error checking for table %s.%s existance: %w", s.cfg.Database(), item.Table, err)
	}
	if exists {
		s.lgr.Infof("Table %s.%s already exists, skip creating it", s.cfg.Database(), item.Table)
		return nil
	}
	return s.dao.CreateTable(s.cfg.Database(), item.Table, item.TableSchema.Columns())
}

func validateRows(items []abstract.ChangeItem) error {
	head := items[0]
	table := head.Table
	schema := head.Schema

	for _, row := range items {
		if row.Kind != abstract.InsertKind {
			return xerrors.Errorf("unsupported row kind %s", row.Kind)
		}
		if row.Table != table || row.Schema != schema {
			return xerrors.Errorf("items batch contains different tables (at least `%s`.`%s` and `%s`.`%s`)",
				schema, table, row.Schema, row.Table)
		}
	}
	return nil
}

func (s *sink) pushDataRows(items []abstract.ChangeItem) (resCh chan error) {
	partID := items[0].TablePartID()
	return s.withBacklog(partID, true, func() error {
		if err := validateRows(items); err != nil {
			return xerrors.Errorf("invalid ChangeItems batch: %w", err)
		}
		part := s.parts.Part(partID)
		if part == nil {
			return xerrors.Errorf("part %s of table %s doesn't exists", partID.PartID, partID.Fqtn())
		}
		if err := part.Append(items); err != nil {
			s.lgr.Error("Append error, closing part",
				log.String("part", partID.PartID), log.String("table", partID.Fqtn()), log.Error(err))
			// failed Append call may leave part in invalid or inconsistent state,
			// so we should close it and release all associated resources
			if closeErr := part.Close(); closeErr != nil {
				s.lgr.Error("Error closing part",
					log.String("part", partID.PartID), log.String("table", partID.Fqtn()), log.Error(closeErr))
			}
			// forget this part thus invalidate all inserts into it
			// note: if InitShardedTableLoad with such partID is received, the part will be created again from scratch
			delete(s.parts, partID)

			if !errors.IsFatalClickhouseError(err) && !db.IsMarshallingError(err) {
				err = abstract.NewRetriablePartUploadError(err)
			}
			return err
		}
		return nil
	})
}

func NewSink(
	transfer *dp_model.Transfer, dst *model.ChDestination, lgr log.Logger, mtrcs metrics.Registry, mw abstract.Middleware,
) (abstract.AsyncSink, error) {
	lgr.Infof("Using async clickhouse sink with parts")
	params, err := dst.ToSinkParams(transfer)
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve sink params: %w", err)
	}
	topology, err := topology.ResolveTopology(params, lgr)
	if err != nil {
		return nil, xerrors.Errorf("error resolving topology: %w", err)
	}
	client, err := NewClusterClient(params, topology, sharding.ShardsFromSinkParams(params.Shards()), lgr)
	if err != nil {
		return nil, xerrors.Errorf("error getting cluster client: %w", err)
	}
	ddlDAO := dao.NewDDLDAO(client, lgr)

	return &sink{
		cl:         client,
		cfg:        params,
		dao:        ddlDAO,
		parts:      make(parts.PartMap),
		resBacklog: make(map[abstract.TablePartID][]chan error),
		transferID: transfer.ID,
		lgr:        lgr,
		sharder:    sharding.CHSharder(params, transfer.ID),
		middleware: NewMiddlewareApplier(mw),
	}, nil
}
