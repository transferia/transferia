package postgres

import (
	"fmt"
)

type DBFlavour interface {
	PgClassFilter() string
	PgClassRelsOnlyFilter() string
	// ListSchemaQuery returns a query with TWO placeholders if withSpecificTable is true; with ZERO placeholders otherwise
	ListSchemaQuery(excludeViews bool, withSpecificTable bool, forbiddenSchemas []string, forbiddenTables []string) string
	// ListTablesQuery returns a query with zero placeholders
	ListTablesQuery(excludeViews bool, forbiddenSchemas []string, forbiddenTables []string) string
}

type PostgreSQLFlavour struct{}

func NewPostgreSQLFlavour() *PostgreSQLFlavour {
	return new(PostgreSQLFlavour)
}

func (f *PostgreSQLFlavour) PgClassFilter() string {
	// https://www.postgresql.org/docs/9.4/catalog-pg-class.html
	// meaning: only allow normal TABLEs, normal VIEWs, and FOREIGN TABLEs, PARTITIONED TABLEs
	return "c.relkind IN ('r', 'v', 'f', 'p')"
}

func (f *PostgreSQLFlavour) PgClassRelsOnlyFilter() string {
	// https://www.postgresql.org/docs/9.4/catalog-pg-class.html
	// meaning: only allow normal tables
	return "c.relkind IN ('r', 'p')"
}

func (f *PostgreSQLFlavour) ListSchemaQuery(excludeViews bool, withSpecificTable bool, forbiddenSchemas []string, forbiddenTables []string) string {
	// See documentation on PostgreSQL service relations and views used in this query:
	// https://www.postgresql.org/docs/9.4/catalog-pg-class.html
	// https://www.postgresql.org/docs/9.4/catalog-pg-namespace.html
	// https://www.postgresql.org/docs/9.4/infoschema-columns.html
	//
	// DO NOT USE SYSTEM VIEW information_schema.element_types!!! TM-5677
	return fmt.Sprintf(`SELECT DISTINCT
	cols.table_schema::TEXT,
	cols.table_name::TEXT,
	cols.column_name::TEXT,
	coalesce(column_default, '') as column_default,
	format_type(pg_attr.atttypid, pg_attr.atttypmod) as data_type,
    (SELECT n.nspname
             FROM pg_type t
             JOIN pg_namespace n ON t.typnamespace = n.oid
             WHERE t.oid = pg_attr.atttypid) AS data_type_schema_name,
	cols.domain_name as domain_name,
	cols.data_type::TEXT as data_type_underlying_under_domain,
	CASE
		WHEN cols.data_type::TEXT = 'USER-DEFINED'
			then array(SELECT e.enumlabel::text FROM pg_catalog.pg_enum e WHERE e.enumtypid = pg_attr.atttypid ORDER BY e.oid)
		WHEN cols.data_type::TEXT = 'ARRAY' and arr.typtype = 'e'
            then array(SELECT e.enumlabel::text
                FROM pg_catalog.pg_enum e
                WHERE e.enumtypid = arr.oid
                ORDER BY e.oid)
	    ELSE null
	END AS all_enum_values,
    CASE
        WHEN cols.is_identity = 'YES'
            THEN 'pg:' || 'GENERATED ' || cols.identity_generation::TEXT || ' AS IDENTITY'
        WHEN cols.is_generated <> 'NEVER'
            THEN 'pg:' || 'GENERATED ' || cols.is_generated::TEXT || ' AS ' || cols.generation_expression::TEXT || ' STORED'
        ELSE ''
    END AS expr,
    cols.ordinal_position AS ordinal_position,
    CASE
        WHEN cols.is_nullable = 'YES' THEN TRUE
        ELSE FALSE
    END AS is_nullable
FROM
    information_schema.columns cols
    INNER JOIN pg_namespace ON pg_namespace.nspname=cols.table_schema
    INNER JOIN pg_class c ON ((c.relnamespace,c.relname)=(pg_namespace.oid,cols.table_name))
    LEFT JOIN pg_attribute pg_attr
        ON (
            (c.oid, cols.column_name) =
            (pg_attr.attrelid, pg_attr.attname)
		)
    LEFT JOIN pg_type arr on arr.typarray = atttypid
WHERE
    has_schema_privilege(current_user, cols.table_schema, 'USAGE')
    AND (%[1]s)
    AND (%[2]s)
ORDER BY
    cols.table_schema::TEXT,
    cols.table_name::TEXT,
    ordinal_position`,
		f.filterForRelationType(excludeViews),
		f.filterForTables(withSpecificTable, forbiddenSchemas, forbiddenTables),
	)
}

func (f *PostgreSQLFlavour) ListTablesQuery(excludeViews bool, forbiddenSchemas []string, forbiddenTables []string) string {
	// See documentation on PostgreSQL service relations and views used in this query:
	// https://www.postgresql.org/docs/9.4/catalog-pg-class.html
	// https://www.postgresql.org/docs/9.4/catalog-pg-namespace.html
	// https://www.postgresql.org/docs/9.4/functions-info.html
	return fmt.Sprintf(`SELECT
    ns.nspname,
    c.relname::TEXT,
    c.relkind::TEXT,
    CASE
        WHEN relkind = 'p' THEN (
            SELECT COALESCE(SUM(child.reltuples), 0)
            FROM
                pg_inherits
                JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child ON pg_inherits.inhrelid = child.oid
            WHERE parent.oid = c.oid
        )
        ELSE c.reltuples
    END
FROM
    pg_class c
    INNER JOIN pg_namespace ns ON c.relnamespace = ns.oid
WHERE
	has_schema_privilege(ns.oid, 'USAGE')
	AND has_table_privilege(c.oid, 'SELECT')
	AND c.relname NOT IN (%[1]s)
    AND ns.nspname NOT IN (%[2]s)
    AND (%[3]s)`,
		ListWithCommaSingleQuoted(forbiddenTables),
		ListWithCommaSingleQuoted(forbiddenSchemas),
		f.filterForRelationType(excludeViews),
	)
}

func (f *PostgreSQLFlavour) filterForRelationType(excludeViews bool) string {
	if excludeViews {
		return f.PgClassRelsOnlyFilter()
	}
	return f.PgClassFilter()
}

func (f *PostgreSQLFlavour) filterForTables(withSpecificTable bool, forbiddenSchemas []string, forbiddenTables []string) string {
	if withSpecificTable {
		return `cols.table_schema = $1 AND cols.table_name = $2`
	}
	return fmt.Sprintf(`cols.table_schema NOT IN (%[1]s) AND cols.table_name NOT IN (%[2]s)`, ListWithCommaSingleQuoted(forbiddenSchemas), ListWithCommaSingleQuoted(forbiddenTables))
}
