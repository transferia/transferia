package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

func queryChildParentRows(ctx context.Context, conn pgxtype.Querier, partitionsOnly bool) (pgx.Rows, error) {
	query := `select c.relname::text AS c_name, cs.nspname::text as c_schema,
					 p.relname::text AS p_name, ps.nspname::text as  p_schema
				from pg_inherits
				inner join pg_class as c on (pg_inherits.inhrelid=c.oid)
				inner join pg_class as p on (pg_inherits.inhparent=p.oid)
				inner join pg_catalog.pg_namespace as ps on (p.relnamespace = ps.oid)
				inner join pg_catalog.pg_namespace as cs on (c.relnamespace = cs.oid)`
	if partitionsOnly {
		query = fmt.Sprintf("%s WHERE c.relispartition", query)
	}
	query += ";"
	return conn.Query(ctx, query)
}

func MakeChildParentMap(ctx context.Context, conn pgxtype.Querier) (map[abstract.TableID]abstract.TableID, error) {
	inheritRows, err := queryChildParentRows(ctx, conn, false)
	if err != nil {
		logger.Log.Error("failed to execute SQL to list inherited tables", log.Error(err))
		return nil, xerrors.Errorf("failed to execute SQL to list inherited tables: %w", err)
	}
	defer inheritRows.Close()

	result := map[abstract.TableID]abstract.TableID{}
	parentChildrenMap := map[abstract.TableID][]abstract.TableID{}
	rootTables := map[abstract.TableID]bool{}
	for inheritRows.Next() {
		var child, childSchema, parent, parentSchema string
		if err := inheritRows.Scan(&child, &childSchema, &parent, &parentSchema); err != nil {
			return nil, err
		}
		childID := abstract.TableID{
			Namespace: childSchema,
			Name:      child,
		}

		parentID := abstract.TableID{
			Namespace: parentSchema,
			Name:      parent,
		}
		result[childID] = parentID
		parentChildrenMap[parentID] = append(parentChildrenMap[parentID], childID)
		delete(rootTables, childID)
		if _, ok := result[parentID]; !ok {
			rootTables[parentID] = true
		}
	}
	if err := inheritRows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to get next row from inherited tables list query: %w", err)
	}

	for rootID := range rootTables {
		for _, childID := range parentChildrenMap[rootID] {
			resolveSubPartitions(parentChildrenMap, result, rootID, childID)
		}
	}

	return result, nil
}

// MakePartitionChildParentMap returns only declarative partition relations (key is child, value is immediate parent).
// Declarative partitioning requires PostgreSQL >= 10 (pg_class.relispartition column).
// On older versions the function returns an empty map because declarative partitions do not exist there.
func MakePartitionChildParentMap(ctx context.Context, conn pgxtype.Querier) (map[abstract.TableID]abstract.TableID, error) {
	var hasRelispartition bool
	err := conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_attribute
			WHERE attrelid = 'pg_catalog.pg_class'::regclass
			  AND attname = 'relispartition'
		)`).Scan(&hasRelispartition)
	if err != nil {
		return nil, xerrors.Errorf("failed to check for pg_class.relispartition column: %w", err)
	}
	if !hasRelispartition {
		logger.Log.Warn("pg_class.relispartition column not found, skipping partition discovery")
		return make(map[abstract.TableID]abstract.TableID), nil
	}

	rows, err := queryChildParentRows(ctx, conn, true)
	if err != nil {
		logger.Log.Error("failed to execute SQL to list partition tables", log.Error(err))
		return nil, xerrors.Errorf("failed to execute SQL to list partition tables: %w", err)
	}
	defer rows.Close()

	result := map[abstract.TableID]abstract.TableID{}
	for rows.Next() {
		var child, childSchema, parent, parentSchema string
		if err := rows.Scan(&child, &childSchema, &parent, &parentSchema); err != nil {
			return nil, xerrors.Errorf("failed to scan partition row: %w", err)
		}
		childID := abstract.TableID{Namespace: childSchema, Name: child}
		parentID := abstract.TableID{Namespace: parentSchema, Name: parent}
		result[childID] = parentID
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to get next row from partition tables list query: %w", err)
	}
	return result, nil
}

func resolveSubPartitions(parentChildrenMap map[abstract.TableID][]abstract.TableID, childParentMap map[abstract.TableID]abstract.TableID, upstreamParentID abstract.TableID, curTableID abstract.TableID) {
	if children, ok := parentChildrenMap[curTableID]; ok {
		for _, childID := range children {
			resolveSubPartitions(parentChildrenMap, childParentMap, upstreamParentID, childID)
		}
	}
	childParentMap[curTableID] = upstreamParentID
}
