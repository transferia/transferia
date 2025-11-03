package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/stringutil"
)

func (s *Storage) canShardBySequenceKeyColumn(key []abstract.ColSchema, table abstract.TableDescription) (bool, error) {
	if len(key) != 1 {
		return false, nil
	}
	isSeq, err := s.isSequence(table.ID(), key[0])
	if err != nil {
		return false, xerrors.Errorf("unable to check table %v for sequence: %w", table.Fqtn(), err)
	}
	return isSeq, nil
}

func (s *Storage) isSequence(table abstract.TableID, col abstract.ColSchema) (bool, error) {
	var seqName interface{}
	if err := s.Conn.QueryRow(
		context.Background(),
		"select pg_get_serial_sequence($1, $2)",
		table.Fqtn(),
		col.ColumnName,
	).Scan(&seqName); err != nil {
		return false, xerrors.Errorf("unable to select pg_get_serial_sequence: %w", err)
	}
	return seqName != nil, nil
}

func (s *Storage) shardBySequenceColumn(ctx context.Context, table abstract.TableDescription, idCol abstract.ColSchema, partCount, etaRows, etaSize uint64) ([]abstract.TableDescription, error) {
	offsetStep := etaRows / partCount
	logger.Log.Infof(
		"Size of table %v (%v) bigger than limit (%v), split in %v parts with %v eta rows in batch",
		table.Fqtn(),
		format.SizeUInt64(etaSize),
		format.SizeUInt64(s.Config.DesiredTableSize),
		partCount,
		offsetStep,
	)

	bounds, err := s.getBoundsBySequenceColumn(ctx, table, idCol, partCount)
	if err != nil {
		return nil, xerrors.Errorf("failed to get bounds for sequence column %s: %w", idCol.ColumnName, err)
	}

	logger.Log.Infof("extracted bounds: %v from table %v", bounds, table.Fqtn())

	shards := make([]abstract.TableDescription, partCount)
	for i := 0; i < int(partCount); i++ {
		var filter string
		if i == 0 {
			filter = fmt.Sprintf(`"%s" < '%v'`, idCol.ColumnName, bounds[i])
		} else if i == int(partCount)-1 {
			filter = fmt.Sprintf(`"%s" >= '%v'`, idCol.ColumnName, bounds[i-1])
		} else {
			filter = fmt.Sprintf(`"%s" >= '%v' AND "%s" < '%v'`, idCol.ColumnName, bounds[i-1], idCol.ColumnName, bounds[i])
		}
		shards[i] = abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Schema,
			Filter: abstract.FiltersIntersection(table.Filter, abstract.WhereStatement(filter)),
			EtaRow: offsetStep,
			Offset: 0,
		}
	}
	logger.Log.Infof("Table %v sharded by sequence column %v into %v parts", table.Fqtn(), idCol.ColumnName, partCount)
	return shards, nil
}

func (s *Storage) getBoundsBySequenceColumn(ctx context.Context, table abstract.TableDescription, idCol abstract.ColSchema, partCount uint64) ([]int64, error) {
	bounds, err := s.getBoundsByPercentileDisc(ctx, table, idCol, partCount)
	if err == nil {
		return bounds, nil
	}

	logger.Log.Warnf("failed to get bounds by percentile disc: %s", err.Error())
	bounds, err = s.getBoundsByMinMax(ctx, table, idCol, partCount)
	if err != nil {
		return nil, xerrors.Errorf("failed to get bounds by min/max: %w", err)
	}
	return bounds, nil
}

func (s *Storage) getBoundsByMinMax(ctx context.Context, table abstract.TableDescription, idCol abstract.ColSchema, partCount uint64) ([]int64, error) {
	logger.Log.Infof("trying to get bounds by min/max")
	min, max, err := s.checkMinMax(ctx, table.ID(), idCol)
	if err != nil {
		return nil, xerrors.Errorf("failed to get min/max: %w", err)
	}

	step := (max - min + 1) / int64(partCount)

	bounds := make([]int64, partCount-1)
	for i := 1; i < int(partCount); i++ {
		bounds[i-1] = min + int64(i)*step
	}
	return bounds, nil
}

func (s *Storage) checkMinMax(ctx context.Context, table abstract.TableID, col abstract.ColSchema) (min, max int64, err error) {
	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return 0, 0, xerrors.Errorf("failed to acquire a connection: %w", err)
	}
	defer conn.Release()

	tx, txRollbacks, err := BeginTxWithSnapshot(ctx, conn.Conn(), repeatableReadReadOnlyTxOptions, s.ShardedStateLSN, logger.Log)
	if err != nil {
		return 0, 0, xerrors.Errorf("failed to start a transaction: %w", err)
	}
	defer txRollbacks.Do()

	err = tx.QueryRow(
		context.Background(),
		fmt.Sprintf(
			`select min(%[1]s)::bigint, max(%[1]s)::bigint from %[2]s`,
			Sanitize(col.ColumnName),
			table.Fqtn(),
		),
	).Scan(&min, &max)

	return min, max, err
}

func (s *Storage) getBoundsByPercentileDisc(ctx context.Context, table abstract.TableDescription, col abstract.ColSchema, partCount uint64) ([]int64, error) {
	percentiles := make([]string, partCount-1)
	for i := 1; i < int(partCount); i++ {
		p := fmt.Sprintf("%f", float64(i)/float64(partCount))
		percentiles[i-1] = p
	}

	percentilesStr := stringutil.JoinStrings(", ", func(p *string) string { return *p }, percentiles...)

	query := fmt.Sprintf(`
		select percentile_disc(ARRAY[%s]) WITHIN GROUP (ORDER BY "%s") from %s;
	`, percentilesStr, col.ColumnName, table.Fqtn())

	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection: %w", err)
	}
	defer conn.Release()

	var rawBounds pgtype.Int8Array
	if err := conn.QueryRow(ctx, query).Scan(&rawBounds); err != nil {
		return nil, xerrors.Errorf("failed to get percentiles: %w", err)
	}

	bounds := make([]int64, partCount-1)
	for i := 0; i < len(rawBounds.Elements); i++ {
		bounds[i] = rawBounds.Elements[i].Int
	}
	return bounds, nil
}
