package provider

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

var (
	_ abstract.ShardingStorage        = (*OracleStorage)(nil)
	_ abstract.ShardingContextStorage = (*OracleStorage)(nil)
)

const (
	// targetBytesPerShard is the approximate byte size of each ROWID-based shard.
	// The table is split into extent-based ranges of ~1 GB each so that every part
	// maps to a disjoint ROWID interval — no full-scan, no ORA_HASH computation.
	targetBytesPerShard uint64 = 1 * 1024 * 1024 * 1024

	// maxShardsPerTable caps the shard count regardless of table size.
	maxShardsPerTable int = 256
)

// rowIDRange holds a single ROWID-based WHERE clause built from dba_extents.
type rowIDRange struct {
	WhereClause string
	Bytes       uint64 // cumulative bytes up to the end of this range
}

// extentRow is a single extent from dba_extents, as consumed by splitByROWID.
type extentRow struct {
	RID   string `db:"RID"`
	Bytes uint64 `db:"BYTES"`
}

// splitByROWID queries dba_extents and returns balanced ROWID-range WHERE clauses.
// Each range covers ≈ bytesPerShard bytes (or targetBytesPerShard if the field is 0).
// Returns nil if dba_extents is inaccessible or the table has fewer than 2 usable ranges.
func (s *OracleStorage) splitByROWID(ctx context.Context, schemaName, tableName string) ([]rowIDRange, error) {
	bytesPerShard := s.rowIDBytesPerShard
	if bytesPerShard == 0 {
		bytesPerShard = targetBytesPerShard
	}
	var extents []extentRow
	queryErr := oracle_common.PDBQueryGlobal(&s.config, s.sqlxDB, ctx,
		func(ctx context.Context, conn *sqlx.Conn) error {
			rows, err := conn.QueryContext(ctx,
				`SELECT DBMS_ROWID.rowid_create(1, obj.data_object_id, ext.relative_fno, ext.block_id, 0) AS rid,
				        ext.bytes
				 FROM dba_extents ext
				 JOIN dba_objects obj ON obj.owner = ext.owner
				   AND obj.object_name = ext.segment_name
				   AND obj.object_type = ext.segment_type
				   AND DECODE(obj.subobject_name, ext.partition_name, 1, 0) = 1
				 WHERE UPPER(ext.owner) = UPPER(:1) AND UPPER(ext.segment_name) = UPPER(:2)
				 ORDER BY ext.relative_fno, ext.block_id`,
				schemaName, tableName)
			if err != nil {
				return xerrors.Errorf("dba_extents query for %q.%q: %w", schemaName, tableName, err)
			}
			defer rows.Close()
			for rows.Next() {
				var e extentRow
				if err := rows.Scan(&e.RID, &e.Bytes); err != nil {
					return xerrors.Errorf("scan dba_extents row: %w", err)
				}
				extents = append(extents, e)
			}
			return rows.Err()
		})
	if queryErr != nil {
		return nil, queryErr
	}
	if len(extents) == 0 {
		return nil, nil
	}

	// Walk extents in ROWID order, accumulate bytes. Every time the running
	// sum crosses bytesPerShard we record a boundary ROWID.
	var boundaries []string
	boundaries = append(boundaries, extents[0].RID)
	var accum, totalBytes uint64
	for _, e := range extents {
		totalBytes += e.Bytes
	}
	for i := 0; i < len(extents); i++ {
		accum += extents[i].Bytes
		if i < len(extents)-1 && accum >= bytesPerShard {
			boundaries = append(boundaries, extents[i+1].RID)
			accum = 0
		}
	}

	if len(boundaries) < 2 {
		return nil, nil // nothing to split
	}

	// Build WHERE clauses from boundary points, track bytes per range
	// so ShardTable can populate EtaRow for natural interleaving.
	ranges := make([]rowIDRange, len(boundaries))
	for i := 0; i < len(boundaries); i++ {
		var where string
		switch {
		case i == 0:
			where = fmt.Sprintf("ROWID < CHARTOROWID('%s')", boundaries[1])
		case i == len(boundaries)-1:
			where = fmt.Sprintf("ROWID >= CHARTOROWID('%s')", boundaries[i])
		default:
			where = fmt.Sprintf("ROWID >= CHARTOROWID('%s') AND ROWID < CHARTOROWID('%s')",
				boundaries[i], boundaries[i+1])
		}
		// Approximate byte size of this range: total bytes / number of ranges.
		// Good enough for CP ORDER BY interleaving and progress estimation.
		ranges[i] = rowIDRange{WhereClause: where, Bytes: totalBytes / uint64(len(boundaries))}
	}
	return ranges, nil
}

type oracleShardingState struct {
	SCN uint64 `json:"scn"`
}

// ShardingContext is called on the main worker to capture the current SCN and share it
// with secondary workers so all workers read from the same consistent snapshot point.
func (s *OracleStorage) ShardingContext() ([]byte, error) {
	scn, err := getCurrentSCN(context.Background(), s.sqlxDB, &s.config)
	if err != nil {
		return nil, xerrors.Errorf("get current SCN for sharding context: %w", err)
	}
	return json.Marshal(oracleShardingState{SCN: scn})
}

// SetShardingContext is called on secondary workers to restore the shared SCN.
func (s *OracleStorage) SetShardingContext(shardedState []byte) error {
	var state oracleShardingState
	if err := json.Unmarshal(shardedState, &state); err != nil {
		return xerrors.Errorf("unmarshal oracle sharding state: %w", err)
	}
	s.shardedSCN = state.SCN
	return nil
}

// ShardTable splits a table for sharded snapshot transfer via ROWID ranges.
// dba_extents is queried to produce balanced, disjoint ROWID intervals of
// ≈ targetBytesPerShard each. Every interval maps to an efficient range scan.
func (s *OracleStorage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	if table.Offset != 0 {
		return nil, abstract.NewNonShardableError(
			xerrors.Errorf("table %v will not be sharded: non-zero offset %v", table.Fqtn(), table.Offset))
	}
	if s.snapshotShardsNum <= 1 {
		return nil, abstract.NewNonShardableError(
			xerrors.Errorf("sharding disabled for table %v (snapshotShardsNum=%v)", table.Fqtn(), s.snapshotShardsNum))
	}

	if err := s.ensureInit(); err != nil {
		return nil, xerrors.Errorf("init: %w", err)
	}

	rowIDRanges, err := s.splitByROWID(ctx, table.Schema, table.Name)
	if err != nil {
		return nil, xerrors.Errorf("ROWID split for table %v: %w", table.Fqtn(), err)
	}
	if len(rowIDRanges) > maxShardsPerTable {
		rowIDRanges = rowIDRanges[:maxShardsPerTable]
	}
	if len(rowIDRanges) < 2 {
		return nil, abstract.NewNonShardableError(
			xerrors.Errorf("table %v cannot be sharded by ROWID (only %d ranges)", table.Fqtn(), len(rowIDRanges)))
	}

	shards := len(rowIDRanges)
	result := make([]abstract.TableDescription, shards)
	for i := 0; i < shards; i++ {
		// Use range bytes + micro-bias (shards-i) so that earlier parts have a
		// slightly higher EtaRow. The CP's ORDER BY eta_rows DESC then naturally
		// interleaves parts from different tables instead of clustering by table name.
		etaRow := rowIDRanges[i].Bytes
		if etaRow > 0 {
			etaRow += uint64(shards - i - 1)
		}
		result[i] = abstract.TableDescription{
			Schema: table.Schema,
			Name:   table.Name,
			Filter: abstract.WhereStatement(rowIDRanges[i].WhereClause),
			EtaRow: etaRow,
			Offset: 0,
		}
	}
	return result, nil
}

// getCurrentSCN reads the current Oracle SCN.
// Tries dbms_flashback.get_system_change_number() first (no DBA privilege required),
// then falls back to v$database (requires SELECT ANY DICTIONARY or DBA).
func getCurrentSCN(ctx context.Context, db *sqlx.DB, config *provider_oracle.OracleSource) (uint64, error) {
	var scn uint64
	err := oracle_common.PDBQueryGlobal(config, db, ctx, func(ctx context.Context, conn *sqlx.Conn) error {
		return conn.GetContext(ctx, &scn, "SELECT dbms_flashback.get_system_change_number() FROM dual")
	})
	if err == nil {
		return scn, nil
	}
	// Fallback: requires SELECT ANY DICTIONARY or DBA role.
	fallbackErr := oracle_common.PDBQueryGlobal(config, db, ctx, func(ctx context.Context, conn *sqlx.Conn) error {
		return conn.GetContext(ctx, &scn, "SELECT current_scn FROM v$database")
	})
	if fallbackErr != nil {
		return 0, xerrors.Errorf("get SCN via dbms_flashback: %w; get SCN via v$database: %w", err, fallbackErr)
	}
	return scn, nil
}
