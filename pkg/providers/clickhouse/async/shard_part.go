package async

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/async/dao"
	"github.com/transferia/transferia/pkg/providers/clickhouse/async/model/db"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type shardPart struct {
	lgr        log.Logger
	db         DDLStreamingClient
	streamer   db.Streamer
	dao        *dao.DDLDAO
	partsDao   *dao.PartsDAO
	baseDB     string
	baseTable  string // Transfer's target table.
	tmpDB      string
	tmpTable   string
	query      string
	marshaller db.ChangeItemMarshaller
	closeOnce  sync.Once
	cols       columntypes.TypeMapping
}

func (s *shardPart) initQueryWMarshaller(row abstract.ChangeItem) error {
	if s.query != "" {
		return nil
	}
	colNames := yslices.Map(row.ColumnNames, func(c string) string { return fmt.Sprintf("`%s`", c) })
	s.query = fmt.Sprintf("INSERT INTO `%s`.`%s` (%s)", s.tmpDB, s.tmpTable, strings.Join(colNames, ","))
	s.marshaller = NewCHV2Marshaller(row.TableSchema.Columns(), s.cols)
	return nil
}

func (s *shardPart) Append(row abstract.ChangeItem) error {
	if err := s.initQueryWMarshaller(row); err != nil {
		return xerrors.Errorf("error initializing query with marshaller: %w", err)
	}
	if s.streamer == nil {
		err := backoff.RetryNotify(func() error {
			strm, err := s.db.StreamInsert(s.query, s.marshaller)
			if err != nil {
				return err
			}
			s.streamer = strm
			return nil
		}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3),
			util.BackoffLoggerWarn(s.lgr, "begin StreamInsert failed, retrying"))
		if err != nil {
			return xerrors.Errorf("error starting insert query: %w", err)
		}
	}
	return s.streamer.Append(row)
}

// Finish loads all data to temporary table and make it ready for Merge.
func (s *shardPart) Finish() error {
	return s.streamer.Finish()
}

// Merge merges temporary table to destination table.
func (s *shardPart) Merge() error {
	defer func() {
		s.lgr.Debug("shardPart closing itself after Commit")
		err := s.Close()
		if err != nil {
			s.lgr.Error("error closing shardPart", log.Error(err))
		}
	}()
	if err := s.partsDao.AttachTablePartsTo(s.baseDB, s.baseTable, s.tmpDB, s.tmpTable); err != nil {
		return xerrors.Errorf("error attaching parts from tmp table: %w", err)
	}
	return s.dao.DropTable(s.tmpDB, s.tmpTable)
}

func (s *shardPart) Close() error {
	// TODO: maybe add drop temporary table here to collect a garbage from clients DB
	var res error
	s.closeOnce.Do(func() {
		if s.streamer != nil {
			s.lgr.Debug("shardPart closing streamer")
			if err := s.streamer.Close(); err != nil {
				res = multierr.Append(res, xerrors.Errorf("error closing streamer: %w", err))
			}
		}
		if s.db != nil {
			if err := s.db.Close(); err != nil {
				res = multierr.Append(res, xerrors.Errorf("error closing db client: %w", err))
			}
		}
	})
	return res
}

func newShardPart(
	lgr log.Logger, baseDB, baseTable, tmpDB, tmpTable, query string, hostDB DDLStreamingClient, cols columntypes.TypeMapping,
) (*shardPart, error) {
	ddldao := dao.NewDDLDAO(hostDB, lgr)
	if err := ddldao.DropTable(tmpDB, tmpTable); err != nil {
		return nil, xerrors.Errorf("error dropping tmp table for part %s.%s: %w", tmpDB, tmpTable, err)
	}
	if err := ddldao.CreateTableAs(baseDB, baseTable, tmpDB, tmpTable); err != nil {
		return nil, xerrors.Errorf("error creating tmp table for part %s.%s: %w", tmpDB, tmpTable, err)
	}
	return &shardPart{
		lgr:        lgr,
		db:         hostDB,
		streamer:   nil,
		dao:        ddldao,
		partsDao:   dao.NewPartsDAO(hostDB, lgr),
		baseDB:     baseDB,
		baseTable:  baseTable,
		tmpDB:      tmpDB,
		tmpTable:   tmpTable,
		query:      query,
		marshaller: nil,
		closeOnce:  sync.Once{},
		cols:       cols,
	}, nil
}
