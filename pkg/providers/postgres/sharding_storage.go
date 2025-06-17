//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"fmt"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/providers/postgres/splitter"
	"github.com/transferia/transferia/pkg/stringutil"
	"github.com/transferia/transferia/pkg/util/set"
)

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

// function 'ShardTable' tries to shard one table/view into parts (split task for snapshotting one table/view into parts):
//
//	TableDescription -> []TableDescription
//
// who can be sharded & who can't be sharded:
//
//   - tables with non-empty 'Offset' - not sharded
//   - tables with non-empty 'Filter' (dolivochki or ad-hoc upload_table) - sharded
//   - views where filled explicitKeys - sharded
func (s *Storage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	// prerequisites

	if table.Offset != 0 {
		return nil, abstract.NewNonShardableError(xerrors.Errorf("Table %v will not be sharded, offset: %v", table.Fqtn(), table.Offset))
	}

	if s.Config.SnapshotDegreeOfParallelism <= 1 {
		return nil, abstract.NewNonShardableError(xerrors.Errorf("Parallel loading disabled due to SnapshotDegreeOfParallelism(%v) option, table %v will be loaded as single shard", s.Config.SnapshotDegreeOfParallelism, table.Fqtn()))
	}

	if s.Config.DesiredTableSize == 0 {
		return nil, xerrors.Errorf("unexpected desired table size: %v, expect > 0", s.Config.DesiredTableSize)
	}

	if s.loadDescending {
		childs, err := s.getChildTables(ctx, table)
		if err != nil {
			logger.Log.Warnf("unable to load child tables: %v", err)
		}
		if len(childs) > 0 {
			return childs, nil
		}
	}

	// harvest metadata

	isView, err := s.isView(ctx, table)
	if err != nil {
		return nil, xerrors.Errorf("unable to determine isView: %w", err)
	}
	hasDataFiltration := table.Filter != ""

	//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	// we have 4 cases here:
	//     - table full
	//     - table dolivochki
	//     - view full
	//     - view dolivochki
	//
	// finally we need after this block:
	//     - uploading data size in bytes
	//     - uploading data size in #rows
	//     - #parts
	//     - 'reason' - why table/view can't be sharded

	currSharder := splitter.BuildSplitter(ctx, s, s.Config.DesiredTableSize, s.Config.SnapshotDegreeOfParallelism, isView, hasDataFiltration)
	splittedTableMetadata, err := currSharder.Split(ctx, table)
	// dataSizeInBytes - can be 0 - for example for views, of if statistics for table is empty
	// dataSizeInRows - can be 0 - for example for views & increments fullscan timeouted
	if err != nil {
		return nil, xerrors.Errorf("table splitter returned an error, err: %w", err)
	}

	//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	table.EtaRow = splittedTableMetadata.DataSizeInRows

	keys, err := s.getTableKeyColumns(ctx, table)
	if err != nil {
		return nil, xerrors.Errorf("failed to define primary key for table %v: %w", table.Fqtn(), err)
	}

	if len(keys) == 0 {
		logger.Log.Warnf("Table %v without primary key will be loaded as single shard", table.Fqtn())
		return []abstract.TableDescription{table}, nil
	}

	canShardBySeqCol, err := s.canShardBySequenceKeyColumn(keys, table)
	if err != nil {
		return nil, xerrors.Errorf("failed to check if table %v could be sharded by sequence column: %w", table.Fqtn(), err)
	}

	// only for snapshots of table with full data
	if canShardBySeqCol && !hasDataFiltration && !isView {
		logger.Log.Infof(
			"Table %v will be sharded by sequence key column %v into %v parts(eta size - %v, eta rows - %v)",
			table.Fqtn(),
			keys[0].ColumnName,
			splittedTableMetadata.PartsCount,
			format.SizeUInt64(splittedTableMetadata.DataSizeInBytes),
			splittedTableMetadata.DataSizeInRows,
		)
		return s.shardBySequenceColumn(ctx, table, keys[0], splittedTableMetadata.PartsCount, splittedTableMetadata.DataSizeInRows, splittedTableMetadata.DataSizeInBytes)
	}
	logger.Log.Infof("Table %v  will be sharded by pkey hash into %v parts", table.Fqtn(), s.Config.SnapshotDegreeOfParallelism)
	return shardByPKHash(table, keys, int32(s.Config.SnapshotDegreeOfParallelism), splittedTableMetadata.DataSizeInRows), nil
}

func (s *Storage) shardBySequenceColumn(ctx context.Context, table abstract.TableDescription, idCol abstract.ColSchema, partCount, etaRows, etaSize uint64) ([]abstract.TableDescription, error) {
	min, max, err := s.checkMinMax(ctx, table.ID(), idCol)
	if err != nil {
		return nil, xerrors.Errorf("unable to check table %v min-max values for column %v: %w", table.Fqtn(), idCol.ColumnName, err)
	}
	var res []abstract.TableDescription

	offsetStep := etaRows / partCount
	idStep := (max - min) / int64(partCount)
	logger.Log.Infof(
		"Size of table %v (%v) bigger than limit (%v), split in %v parts with %v eta rows in batch",
		table.Fqtn(),
		format.SizeUInt64(etaSize),
		format.SizeUInt64(s.Config.DesiredTableSize),
		partCount,
		offsetStep,
	)
	res = append(res, abstract.TableDescription{
		Name:   table.Name,
		Schema: table.Schema,
		Filter: abstract.WhereStatement(fmt.Sprintf(`"%[1]v" < %[2]v`, idCol.ColumnName, min+idStep)),
		EtaRow: offsetStep,
		Offset: 0,
	})
	for i := int64(1); i < int64(partCount)-1; i++ {
		res = append(res, abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Schema,
			Filter: abstract.WhereStatement(fmt.Sprintf(`"%[1]v" >= %[2]v and "%[1]v" < %[3]v`, idCol.ColumnName, min+idStep*i, min+idStep*(i+1))),
			EtaRow: offsetStep,
			Offset: 0,
		})
	}
	res = append(res, abstract.TableDescription{
		Name:   table.Name,
		Schema: table.Schema,
		Filter: abstract.WhereStatement(fmt.Sprintf(`"%[1]v" >= %[2]v`, idCol.ColumnName, min+idStep*(int64(partCount)-1))),
		EtaRow: offsetStep,
		Offset: 0,
	})

	logger.Log.Infof("Table %v had sequences, split into shards: %v", table.Fqtn(), len(res))
	return res, nil
}

func (s *Storage) getLoadTableMode(ctx context.Context, table abstract.TableDescription) (*loadTableMode, error) {
	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection from pool: %w", err)
	}
	defer conn.Release()
	return s.discoverTableLoadMode(ctx, conn, table)
}

func (s *Storage) isView(ctx context.Context, table abstract.TableDescription) (bool, error) {
	loadMode, err := s.getLoadTableMode(ctx, table)
	if err != nil {
		return false, xerrors.Errorf("unable to get load table mode: %w", err)
	}
	return loadMode.tableInfo.IsView, nil
}

func (s *Storage) getTableKeyColumns(ctx context.Context, table abstract.TableDescription) ([]abstract.ColSchema, error) {
	columns, err := func() (*abstract.TableSchema, error) {
		if conn, err := s.Conn.Acquire(ctx); err != nil {
			return nil, xerrors.Errorf("failed to acquire a connection from pool: %w", err)
		} else {
			defer conn.Release()
			return s.LoadSchemaForTable(ctx, conn.Conn(), table)
		}
	}()
	if err != nil {
		return nil, xerrors.Errorf("failed to load schema: %w", err)
	}

	logger.Log.Infof("Got table %v columns: %v", table.Fqtn(), columns.ColumnNames())
	var keys []abstract.ColSchema
	explicitShardingKeyFields := s.explicitShardingKeys(table)

	if len(explicitShardingKeyFields) > 0 { // Use explicitly specified sharing keys if present
		shardingKeyFieldsSet := set.New[string](explicitShardingKeyFields...)
		for _, col := range columns.Columns() {
			if shardingKeyFieldsSet.Contains(col.ColumnName) {
				keys = append(keys, col)
			}
		}
	} else { // Else use PKs
		for _, col := range columns.Columns() {
			if col.PrimaryKey {
				keys = append(keys, col)
			}
		}
	}

	return keys, nil
}

func (s *Storage) explicitShardingKeys(table abstract.TableDescription) []string {
	logger.Log.Infof("Got all transfer explicit sharding fields: %v", s.Config.ShardingKeyFields)
	for tableName, keys := range s.Config.ShardingKeyFields {
		if tableName == table.Fqtn() {
			logger.Log.Infof("Got table %v explicit sharding fields: %v", table.Fqtn(), keys)
			return keys
		}

		if userTable, err := abstract.NewTableIDFromStringPg(tableName, table.ID().Namespace != ""); err == nil {
			logger.Log.Infof("userTable %v", userTable)
			if userTable.Name == table.Name && userTable.Namespace == table.ID().Namespace {
				logger.Log.Infof("Got table %v explicit sharding fields: %v", table.Fqtn(), keys)
				return keys
			}
		} else {
			logger.Log.Errorf("Can't parse table name: %v into PG table ID, error: %v", tableName, err)
		}

	}
	logger.Log.Infof("Got table %v explicit sharding fields: []", table.Fqtn())
	return []string{}
}

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

func shardByPKHash(table abstract.TableDescription, keys []abstract.ColSchema, shardCount int32, etaRows uint64) []abstract.TableDescription {
	cols := stringutil.JoinStrings(",", func(col *abstract.ColSchema) string { return fmt.Sprintf("\"%v\"", col.ColumnName) }, keys...)

	shardEtaSize := etaRows / uint64(shardCount)

	shards := make([]abstract.TableDescription, shardCount)
	for i := int32(0); i < shardCount; i++ {
		shardFilter := abstract.WhereStatement(fmt.Sprintf("abs(('x'||substr(md5(row(%v)::text),1,8))::bit(16)::int) %% %v = %v", cols, shardCount, i))
		shards[i] = abstract.TableDescription{
			Name:   table.Name,
			Schema: table.ID().Namespace,
			Filter: abstract.FiltersIntersection(table.Filter, shardFilter),
			EtaRow: shardEtaSize,
			Offset: 0,
		}
		logger.Log.Infof("shard %v for %v: %v", i, table.Fqtn(), shards[i].Filter)
	}
	return shards
}

func CalculatePartCount(totalSize, desiredPartSize, partCountLimit uint64) uint64 {
	partCount := totalSize / desiredPartSize
	if totalSize%desiredPartSize > 0 {
		partCount += 1
	}
	if partCount > partCountLimit {
		partCount = partCountLimit
	}
	return partCount
}
