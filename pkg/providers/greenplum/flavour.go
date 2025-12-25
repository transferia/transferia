package greenplum

import (
	"fmt"

	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
)

type GreenplumFlavour struct {
	pgClassFilter         func(bool, func() string) string
	pgClassRelsOnlyFilter func() string

	coordinatorOnlyMode bool
}

func NewGreenplumFlavourImpl(coordinatorOnlyMode bool, pgClassFilter func(bool, func() string) string, pgClassRelsOnlyFilter func() string) *GreenplumFlavour {
	return &GreenplumFlavour{
		pgClassFilter:         pgClassFilter,
		pgClassRelsOnlyFilter: pgClassRelsOnlyFilter,
		coordinatorOnlyMode:   coordinatorOnlyMode,
	}
}

// NewGreenplumFlavour constructs a flavour for PostgreSQL schema extractor
func NewGreenplumFlavour(coordinatorOnlyMode bool) *GreenplumFlavour {
	return NewGreenplumFlavourImpl(coordinatorOnlyMode, pgClassFilter, pgClassRelsOnlyFilter)
}

func pgClassFilter(coordinatorOnlyMode bool, pgClassRelsOnlyFilter func() string) string {
	if coordinatorOnlyMode {
		// https://gpdb.docs.pivotal.io/6-19/ref_guide/system_catalogs/pg_class.html
		// meaning: allow normal tables of all kinds, VIEWs, FOREIGN and EXTERNAL tables
		return "c.relkind IN ('r', 'v', 'f') AND c.relstorage IN ('a', 'c', 'h', 'v', 'x')"
	}
	return pgClassRelsOnlyFilter()
}

func (f *GreenplumFlavour) PgClassFilter() string {
	return f.pgClassFilter(f.coordinatorOnlyMode, f.pgClassRelsOnlyFilter)
}

func pgClassRelsOnlyFilter() string {
	// https://gpdb.docs.pivotal.io/6-19/ref_guide/system_catalogs/pg_class.html
	// meaning: only allow normal tables of all kinds
	return "c.relkind = 'r' AND c.relstorage IN ('a', 'c', 'h', 'v')"
}

func (f *GreenplumFlavour) PgClassRelsOnlyFilter() string {
	return pgClassRelsOnlyFilter()
}

func (f *GreenplumFlavour) ListSchemaQuery(excludeViews bool, withSpecificTable bool, forbiddenSchemas []string, forbiddenTables []string) string {
	return fmt.Sprintf(`WITH ic_columns AS (
    SELECT
		nc.nspname::information_schema.sql_identifier AS table_schema,
        c.relname::information_schema.sql_identifier AS table_name,
        a.attname::information_schema.sql_identifier AS column_name,
        a.attnum::information_schema.cardinal_number AS ordinal_position,
        format_type(a.atttypid, a.atttypmod) as data_type,
		(SELECT n.nspname FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid
				 WHERE t.oid = a.atttypid) AS data_type_schema_name,
        CASE
            WHEN nbt.nspname = 'pg_catalog'::name THEN format_type(bt.oid, NULL::integer)
            ELSE 'USER-DEFINED'::text
        END::information_schema.character_data AS type_data_type,
        information_schema._pg_char_max_length(
            information_schema._pg_truetypid(a.*, t.*),
            information_schema._pg_truetypmod(a.*, t.*)
        )::information_schema.cardinal_number AS character_maximum_length,
        information_schema._pg_numeric_precision(
            information_schema._pg_truetypid(a.*, t.*),
            information_schema._pg_truetypmod(a.*, t.*)
        )::information_schema.cardinal_number AS numeric_precision,
        information_schema._pg_numeric_scale(
            information_schema._pg_truetypid(a.*, t.*),
            information_schema._pg_truetypmod(a.*, t.*)
        )::information_schema.cardinal_number AS numeric_scale,
        information_schema._pg_datetime_precision(
            information_schema._pg_truetypid(a.*, t.*),
            information_schema._pg_truetypmod(a.*, t.*)
        )::information_schema.cardinal_number AS datetime_precision,
        coalesce(nbt.nspname, nt.nspname)::information_schema.sql_identifier AS udt_schema,
        coalesce(bt.typname, t.typname)::information_schema.sql_identifier AS udt_name,
        'NO'::character varying::information_schema.yes_or_no AS is_identity,
        NULL::character varying::information_schema.character_data AS identity_generation,
        'NEVER'::character varying::information_schema.character_data AS is_generated,
        NULL::character varying::information_schema.character_data AS generation_expression,
        format_type(a.atttypid, a.atttypmod) AS data_type_verbose,
        a.attnotnull AS is_required
    FROM pg_attribute a
        LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid
        AND a.attnum = ad.adnum
        JOIN (
            pg_class c
            JOIN pg_namespace nc ON c.relnamespace = nc.oid
        ) ON a.attrelid = c.oid
        JOIN (
            pg_type t
            JOIN pg_namespace nt ON t.typnamespace = nt.oid
        ) ON a.atttypid = t.oid
        LEFT JOIN (
            pg_type bt
            JOIN pg_namespace nbt ON bt.typnamespace = nbt.oid
        ) ON t.typtype = 'd'::"char"
        AND t.typbasetype = bt.oid
        LEFT JOIN (
            pg_collation co
            JOIN pg_namespace nco ON co.collnamespace = nco.oid
        ) ON a.attcollation = co.oid
        AND (
            nco.nspname <> 'pg_catalog'::name
            OR co.collname <> 'default'::name
        )
    WHERE NOT pg_is_other_temp_schema(nc.oid)
        AND a.attnum > 0
        AND NOT a.attisdropped
        AND (%[1]s)
        AND (
            pg_has_role(c.relowner, 'USAGE'::text)
            OR has_column_privilege(
                c.oid,
                a.attnum,
                'SELECT, INSERT, UPDATE, REFERENCES'::text
            )
        )
        AND (%[2]s)
)
SELECT
	table_schema::text,
    table_name::text,
    column_name::text,
    '' as column_default,
    data_type::text,
	data_type_schema_name::text,
    null as domain_name,
    data_type_verbose::text as data_type_underlying_under_domain,
    null as all_enum_values,
    CASE
        WHEN is_identity = 'YES'
			THEN 'pg:' || 'GENERATED ' || identity_generation::text || ' AS IDENTITY'
        WHEN is_generated <> 'NEVER'
			THEN 'pg:' || 'GENERATED ' || is_generated::text || ' AS ' || generation_expression::text || ' STORED'
        ELSE ''
    END AS expr,
    ordinal_position,
    not is_required as nullable
FROM ic_columns
ORDER BY
    table_schema::text,
    table_name::text,
    ordinal_position;`,
		f.filterForRelationType(excludeViews),
		f.filterForTables(withSpecificTable, forbiddenTables, forbiddenSchemas),
	)
}

func (f *GreenplumFlavour) ListTablesQuery(excludeViews bool, forbiddenSchemas []string, forbiddenTables []string) string {
	// See documentation on PostgreSQL service relations and views used in this query:
	// https://gpdb.docs.pivotal.io/6-19/ref_guide/system_catalogs/pg_class.html
	// https://gpdb.docs.pivotal.io/6-19/ref_guide/system_catalogs/pg_namespace.html
	// https://www.postgresql.org/docs/9.4/functions-info.html
	// https://gpdb.docs.pivotal.io/6-19/ref_guide/system_catalogs/gp_distribution_policy.html
	return fmt.Sprintf(
		f.baseListTablesQuery(),
		pgcommon.ListWithCommaSingleQuoted(forbiddenTables),
		pgcommon.ListWithCommaSingleQuoted(forbiddenSchemas),
		f.filterForRelationType(excludeViews),
	)
}

func (f *GreenplumFlavour) filterForRelationType(excludeViews bool) string {
	if excludeViews {
		return f.PgClassRelsOnlyFilter()
	}
	return f.PgClassFilter()
}

func (f *GreenplumFlavour) filterForTables(withSpecificTable bool, forbiddenTables []string, forbiddenSchemas []string) string {
	if withSpecificTable {
		return `nc.nspname = $1 AND c.relname = $2`
	}
	return fmt.Sprintf(`nc.nspname NOT IN (%[1]s) AND c.relname NOT IN (%[2]s)`, pgcommon.ListWithCommaSingleQuoted(forbiddenSchemas), pgcommon.ListWithCommaSingleQuoted(forbiddenTables))
}

func (f *GreenplumFlavour) baseListTablesQuery() string {
	if f.coordinatorOnlyMode {
		return baseListTablesQueryCoordinatorOnly
	}
	return baseListTablesQueryDistributed
}

const baseListTablesQueryCoordinatorOnly string = `SELECT
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
    AND (%[3]s)`

const baseListTablesQueryDistributed string = `SELECT
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
    INNER JOIN pg_catalog.gp_distribution_policy dp ON c.oid = dp.localoid
WHERE
    has_schema_privilege(ns.oid, 'USAGE')
    AND has_table_privilege(c.oid, 'SELECT')
    AND c.relname NOT IN (%[1]s)
    AND ns.nspname NOT IN (%[2]s)
    AND (%[3]s)
    AND dp.policytype = 'p'`
