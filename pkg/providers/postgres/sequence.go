//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/randutil"
)

const (
	createOIDEvalFnQuery = `CREATE FUNCTION %s(expr TEXT)
    RETURNS OID AS
$$
DECLARE
    id OID;
BEGIN
    BEGIN
        EXECUTE format('SELECT %%s', expr) INTO id;
    EXCEPTION
        WHEN OTHERS THEN
            SELECT 0 INTO id;
    END;
    RETURN id;
END
$$ LANGUAGE plpgsql`
	dropOIDEvalFnQuery              = `DROP FUNCTION %s(expr TEXT)`
	listSequencesAndDependentsQuery = `SELECT
    nsp.nspname      AS sequence_schema,
    seq.relname      AS sequence_name,
    dep.table_schema AS table_schema,
    dep.table_name   AS table_name
FROM pg_class seq
JOIN pg_namespace nsp ON seq.relnamespace = nsp.oid
LEFT JOIN (SELECT
               sequence_id,
               nsp.nspname AS table_schema,
               tbl.relname AS table_name
           FROM (SELECT
                     objid    AS sequence_id,
                     refobjid AS table_id
                 FROM pg_depend
                 UNION
                 SELECT
                     %s(
                             substring(def_expr,
                                       length('nextval(') + 1,
                                       length(def_expr) - length('nextval(') - length(')'))
                         ) AS sequence_id,
                     table_id
                 FROM (SELECT
                           pg_get_expr(def.adbin, def.adrelid) AS def_expr,
                           attrelid                            AS table_id
                       FROM pg_attribute attr
                       JOIN pg_attrdef def ON (attr.attrelid, attr.attnum) = (def.adrelid, def.adnum)) AS dep
                 WHERE def_expr LIKE 'nextval(%%)') AS dep
           JOIN pg_class tbl ON dep.table_id = tbl.oid
           JOIN pg_namespace nsp ON tbl.relnamespace = nsp.oid) AS dep
    ON seq.oid = dep.sequence_id
WHERE seq.relkind = 'S'`
	getSequenceDataQuery = `SELECT last_value, is_called FROM %s`
)

// SequenceInfo is a description of a PostgreSQL sequence
type SequenceInfo struct {
	SequenceID      abstract.TableID
	DependentTables []abstract.TableID
}

func newSequenceInfo(sequenceID abstract.TableID) *SequenceInfo {
	return &SequenceInfo{
		SequenceID:      sequenceID,
		DependentTables: nil,
	}
}

// SequenceMap is a mapping of sequence identifiers to their descriptions
type SequenceMap map[abstract.TableID]*SequenceInfo

// ListSequencesWithDependants returns a mapping with sequence information.
//
// This method executes CREATE FUNCTION and DROP FUNCTION statements.
func ListSequencesWithDependants(ctx context.Context, conn *pgx.Conn, serviceSchema string) (SequenceMap, error) {
	oidEvalFnName := abstract.NewTableID(serviceSchema, fmt.Sprintf("dt_eval_oid_%s", randutil.GenerateAlphanumericString(10))).Fqtn()

	_, err := conn.Exec(ctx, fmt.Sprintf(createOIDEvalFnQuery, oidEvalFnName))
	if err != nil {
		return nil, xerrors.Errorf("unable to create OID evaluation function: %w", err)
	}

	result := SequenceMap{}

	rows, err := conn.Query(ctx, fmt.Sprintf(listSequencesAndDependentsQuery, oidEvalFnName))
	if err != nil {
		return nil, xerrors.Errorf("unable to query sequences: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var seqID abstract.TableID
		var tableNamespace, tableName sql.NullString
		err = rows.Scan(&seqID.Namespace, &seqID.Name, &tableNamespace, &tableName)
		if err != nil {
			return nil, xerrors.Errorf("unable to scan row: %w", err)
		}
		if result[seqID] == nil {
			result[seqID] = newSequenceInfo(seqID)
		}
		info := result[seqID]
		if tableNamespace.Valid && tableName.Valid {
			info.DependentTables = append(info.DependentTables, *abstract.NewTableID(tableNamespace.String, tableName.String))
		}
	}
	if rows.Err() != nil {
		return nil, xerrors.Errorf("unable to read row: %w", rows.Err())
	}

	_, err = conn.Exec(ctx, fmt.Sprintf(dropOIDEvalFnQuery, oidEvalFnName))
	if err != nil {
		return nil, xerrors.Errorf("unable to drop eval function: %w", err)
	}

	return result, nil
}

func GetCurrentStateOfSequence(ctx context.Context, conn *pgx.Conn, sequenceID abstract.TableID) (lastValue int64, isCalled bool, err error) {
	if err := conn.QueryRow(ctx, fmt.Sprintf(getSequenceDataQuery, sequenceID.Fqtn())).Scan(&lastValue, &isCalled); err != nil {
		return 0, false, xerrors.Errorf("failed to SELECT FROM SEQUENCE: %w", err)
	}
	return lastValue, isCalled, nil
}
