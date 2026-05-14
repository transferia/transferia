package snapshot

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type oracleTableSource struct {
	sqlxDB   *sqlx.DB
	config   *provider_oracle.OracleSource
	position *oracle_common.LogPosition
	table    *oracle_schema.Table
	load     *loader
	logger   log.Logger
}

func NewTableSource(
	sqlxDB *sqlx.DB,
	config *provider_oracle.OracleSource,
	position *oracle_common.LogPosition,
	table *oracle_schema.Table,
	logger log.Logger,
	sourceStats *stats.SourceStats,
) (*oracleTableSource, error) {
	if position != nil && !position.OnlySCN() {
		return nil, xerrors.Errorf("position error: Can start from SCN only")
	}

	return &oracleTableSource{
		sqlxDB:   sqlxDB,
		config:   config,
		position: position,
		table:    table,
		load:     newLoader(sqlxDB, config, position, table, logger, sourceStats),
		logger:   logger,
	}, nil
}

func (s *oracleTableSource) Load(ctx context.Context, pusher abstract.Pusher) error {
	columnsSQL, err := getSelectColumns(s.table)
	if err != nil {
		return xerrors.Errorf("Can't create select columns SQL for table '%v': %w", s.table.OracleSQLName(), err)
	}
	var sqlQuery string
	var scn uint64
	if s.position != nil {
		scn = s.position.SCN()
	}
	if s.config.IsNonConsistentSnapshot || s.position == nil {
		sqlQuery = fmt.Sprintf("select %v from %v", columnsSQL, s.table.OracleSQLName())
	} else {
		sqlQuery = fmt.Sprintf("select %v from %v as of scn %v", columnsSQL, s.table.OracleSQLName(), s.position.SCN())
	}
	s.logger.Info("Oracle: starting snapshot for table",
		log.String("table", s.table.OracleSQLName()),
		log.UInt64("scn", scn),
		log.Bool("consistent", !s.config.IsNonConsistentSnapshot && s.position != nil),
		log.String("query", util.Sample(sqlQuery, 4*1024)))
	if err := s.load.LoadSnapshot(ctx, pusher, sqlQuery); err != nil {
		return xerrors.Errorf("failed while loading snapshot: %w", err)
	}
	return nil
}
