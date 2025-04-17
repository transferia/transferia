package alltypes

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/dblog"
	"github.com/transferia/transferia/pkg/dblog/tablequery"
	"github.com/transferia/transferia/pkg/providers/postgres"
	pg_dblog "github.com/transferia/transferia/pkg/providers/postgres/dblog"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

const (
	defaultLimit = 1
)

var (
	Source                       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""))
	repeatableReadWriteTxOptions = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable}

	postgresTypes = []string{
		//	"hstore", cannot be a primary key

		"boolean",
		"bit",
		"varbit",

		"smallint",
		"smallserial",
		"integer",
		"serial",
		"bigint",
		"bigserial",
		"oid",
		"tid",

		"real",
		"double precision",

		"char",
		"varchar",
		"name",

		"character",
		"character varying",
		"timestamptz",
		"timestamp with time zone",
		"timestamp without time zone",
		"timetz",
		"time with time zone",
		"time without time zone",
		"interval",

		"bytea",

		// "json", cannot be a primary key
		"jsonb",
		// "xml", cannot be a primary key

		"uuid",
		// "point", cannot be a primary key
		"inet",
		"int4range",
		"int8range",
		"numrange",
		"tsrange",
		"tstzrange",
		"daterange",

		"float",
		"int",
		"text",

		"date",
		"time",

		"numeric",
		"decimal",
		"money",

		"cidr",
		"macaddr",
		"citext",

		"_jsonb",
		"_numeric",
		"_text",
		"_timestamp",
		"_timestamptz",
		// "_tsrange",  failed to execute SELECT: ERROR: malformed range literal: "[2023-01-01 00:00:00" (SQLSTATE 22P02)
		// "_tstzrange", failed to execute SELECT: ERROR: malformed range literal: "[2023-01-01 00:00:00Z" (SQLSTATE 22P02)
		"_uuid",
		"_varchar",
		// "_aclitem", could not identify a comparison function for type aclitem
		"_bool",
		"_bpchar",
		"_bytea",
		"_cidr",
		"_date",
		"_float4",
		"_float8",
		"_inet",
		"_int2",
		"_int4",
		"_int8",
		"bpchar",
		"float4",
		"float8",
		"int2",
		"int4",
		"int8",
		"bool",

		// pg 14+
		"nummultirange",
		"int4multirange",
		"int8multirange",
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestIncrementalSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	storage, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)

	conn, err := storage.Conn.Acquire(context.TODO())
	require.NoError(t, err)

	tx, err := conn.BeginTx(context.TODO(), repeatableReadWriteTxOptions)
	require.NoError(t, err)

	tableNames := make([]string, 0, len(postgresTypes))

	for _, pgType := range postgresTypes {
		require.Equal(t, dblog.TypeSupported, pg_dblog.CheckTypeCompatibility(pgType), "pgType: %s", pgType)

		tableName := createTableNameForType(pgType)
		tableNames = append(tableNames, tableName)

		_, err := tx.Exec(context.TODO(), createTableWithPkTypeQuery(pgType))
		require.NoError(t, err)
	}

	inserts := map[string][]string{
		"boolean_pk_table": {"'false'", "'true'"},
		"bit_pk_table":     {"'0'", "'1'"},
		"varbit_pk_table":  {"'00000000'", "'11111111'"},

		"smallint_pk_table":    {"'1'", "'2'"},
		"smallserial_pk_table": {"'1'", "'2'"},
		"integer_pk_table":     {"'1'", "'2'"},
		"serial_pk_table":      {"'1'", "'2'"},
		"bigint_pk_table":      {"'100'", "'200'"},
		"bigserial_pk_table":   {"'100'", "'200'"},
		"oid_pk_table":         {"'1'", "'2'"},

		"real_pk_table":             {"'1.1'", "'2.2'"},
		"double precision_pk_table": {"'1.1'", "'2.2'"},

		"char_pk_table":    {"'A'", "'B'"},
		"varchar_pk_table": {"'alpha'", "'beta'"},
		"name_pk_table":    {"'A'", "'B'"},

		"character_pk_table":                   {"'A'", "'B'"},
		"character varying_pk_table":           {"'alpha'", "'beta'"},
		"timestamptz_pk_table":                 {"'2023-01-01 00:00:00+03:00:00'", "'2023-01-02 00:00:00+03:00:00'"},
		"timestamp with time zone_pk_table":    {"'2023-01-01 00:00:00+03:00:00'", "'2023-01-02 00:00:00+03:00:00'"},
		"timestamp without time zone_pk_table": {"'2023-01-01 00:00:00+03:00:00'", "'2023-01-02 00:00:00+03:00:00'"},
		"timetz_pk_table":                      {"'00:00:00+03'", "'01:00:00+03'"},
		"time with time zone_pk_table":         {"'00:00:00+03'", "'01:00:00+03'"},
		"time without time zone_pk_table":      {"'00:00:00+03'", "'01:00:00+03'"},
		"interval_pk_table":                    {"'1 day'", "'2 day'"},
		"bytea_pk_table":                       {"'\\x00'", "'\\xff'"},

		"jsonb_pk_table": {"'{}'", "'{\"key\":\"value\"}'"},

		"uuid_pk_table":      {"'550e8400-e29b-41d4-a716-446655440000'", "'550e8400-e29b-41d4-a716-446655440001'"},
		"inet_pk_table":      {"'192.168.1.1'", "'192.168.1.2'"},
		"int4range_pk_table": {"'[1,10)'", "'[2,20)'"},
		"int8range_pk_table": {"'[1,100)'", "'[2,200)'"},
		"numrange_pk_table":  {"'(15e-1,25e-1)'", "'(25e-1,35e-1)'"},
		"tsrange_pk_table":   {"'[2023-01-01 00:00:00,2023-01-01 01:00:00)'", "'[2023-01-02 00:00:00,2023-01-02 01:00:00)'"},
		"tstzrange_pk_table": {"'[2023-01-01 00:00:00Z,2023-01-01 01:00:00Z)'", "'[2023-01-02 00:00:00Z,2023-01-02 01:00:00Z)'"},
		"daterange_pk_table": {"'[2023-01-01,2023-01-10)'", "'[2023-01-02,2023-01-20)'"},

		"float_pk_table": {"'1.1'", "'2.2'"},
		"int_pk_table":   {"'1'", "'2'"},
		"text_pk_table":  {"'alpha'", "'beta'"},

		"date_pk_table":    {"'2023-01-01'", "'2023-01-02'"},
		"time_pk_table":    {"'00:00:00'", "'01:00:00'"},
		"numeric_pk_table": {"'11e-1'", "'22e-1'"},
		"decimal_pk_table": {"'11e-1'", "'22e-1'"},
		"money_pk_table":   {"'$1.11'", "'$2.22'"},
		"cidr_pk_table":    {"'192.168.1.0/24'", "'192.168.2.0/24'"},
		"macaddr_pk_table": {"'08:00:2b:01:02:03'", "'08:00:2b:01:02:04'"},
		"citext_pk_table":  {"'example'", "'test'"},

		"tid_pk_table":    {"'(0,1)'", "'(0,2)'"},
		"bpchar_pk_table": {"'A'", "'B'"},
		"float4_pk_table": {"'1.1'", "'2.2'"},
		"float8_pk_table": {"'1.1'", "'2.2'"},
		"int2_pk_table":   {"'1'", "'2'"},
		"int4_pk_table":   {"'1'", "'2'"},
		"int8_pk_table":   {"'100'", "'200'"},
		"bool_pk_table":   {"'false'", "'true'"},

		// pg 14+
		"nummultirange_pk_table":  {"'{(15e-1,25e-1), (25e-1,35e-1)}'", "'{(20e-1,30e-1), (30e-1,40e-1)}'"},
		"int4multirange_pk_table": {"'{[3,7), [8,9)}'", "'{[4,8), [9,10)}'"},
		"int8multirange_pk_table": {"'{[1,100), [200,300)}'", "'{[100,200), [300,400)}'"},

		"_jsonb_pk_table":       {"'{1, 2, 3}'", "'{4, 5, 6}'"},
		"_numeric_pk_table":     {"ARRAY['1.1', '2.2']::numeric[]", "ARRAY['3.3', '4.4']::numeric[]"},
		"_text_pk_table":        {"ARRAY['alpha', 'beta']::text[]", "ARRAY['gamma', 'delta']::text[]"},
		"_timestamp_pk_table":   {"ARRAY['2023-01-01 00:00:00', '2023-01-02 00:00:00']::timestamp[]", "ARRAY['2023-01-03 00:00:00']::timestamp[]"},
		"_timestamptz_pk_table": {"'{2023-01-01 00:00:00+03, 2023-01-02 00:00:00+03}'", "'{2023-01-03 00:00:00+03, 2023-01-04 00:00:00+03}'"},
		//  failed to execute SELECT:	ERROR: malformed range literal: "[2023-01-01 00:00:00" (SQLSTATE 22P02)
		// "_tsrange_pk_table":     {"'{\"[2023-01-01 00:00:00,2023-01-01 01:00:00)\"}'::tsrange[]", "'{\"[2023-01-02 00:00:00,2023-01-02 01:00:00)\"}'::tsrange[]"}
		// "_tstzrange_pk_table": {"'{\"[2023-01-01 00:00:00Z,2023-01-01 01:00:00Z)\"}'", "'{\"[2023-01-02 00:00:00Z,2023-01-02 01:00:00Z)\"}'"},
		"_uuid_pk_table":    {"'{550e8400-e29b-41d4-a716-446655440000, 550e8400-e29b-41d4-a716-446655440001}'", "'{550e8400-e29b-41d4-a716-446655440002, 550e8400-e29b-41d4-a716-446655440003}'"},
		"_varchar_pk_table": {"'{alpha, beta}'", "'{gamma, delta}'"},
		"_bool_pk_table":    {"'{false, true}'", "'{true, true}'"},
		"_bpchar_pk_table":  {"'{A, B}'", "'{C, D}'"},
		"_bytea_pk_table":   {"'{\\x00, \\xff}'", "'{\\xaa}'"},
		"_cidr_pk_table":    {"'{192.168.1.0/24, 192.168.2.0/24}'", "'{192.168.3.0/24}'"},
		"_date_pk_table":    {"'{2023-01-01, 2023-01-02}'", "'{2023-01-03}'"},
		"_float4_pk_table":  {"'{1.1, 2.2}'", "'{3.3, 4.4}'"},
		"_float8_pk_table":  {"'{1.1, 2.2}'", "'{3.3, 4.4}'"},
		"_inet_pk_table":    {"'{192.168.1.1, 192.168.1.2}'", "'{192.168.1.3}'"},
		"_int2_pk_table":    {"'{1, 2}'", "'{3, 4}'"},
		"_int4_pk_table":    {"'{1, 2}'", "'{3, 4}'"},
		"_int8_pk_table":    {"'{100, 200}'", "'{300, 400}'"},
	}

	keys := make([]string, 0, len(inserts))
	tableNamesInInserts := make([]string, 0, len(inserts))
	for key := range inserts {
		keys = append(keys, strings.TrimSuffix(key, "_pk_table"))
		tableNamesInInserts = append(tableNamesInInserts, key)
	}

	require.ElementsMatch(t, postgresTypes, keys)
	require.ElementsMatch(t, tableNamesInInserts, tableNames)

	for _, tableName := range tableNames {
		_, err := tx.Exec(context.TODO(), insertQueryValues(tableName, addIdxToPk(inserts[tableName])))
		require.NoError(t, err)
	}

	err = tx.Commit(context.TODO())
	require.NoError(t, err)

	sourceTables, err := storage.TableList(nil)
	require.NoError(t, err)

	tables := sourceTables.ConvertToTableDescriptions()

	TableNameToDescription := make(map[string]abstract.TableDescription)

	for _, table := range tables {
		TableNameToDescription[table.Name] = table
	}

	primaryKey := []string{"pk"}

	for _, pgType := range postgresTypes {
		tableName := createTableNameForType(pgType)
		tableDescription := TableNameToDescription[tableName]

		signalTable := dblog.NewMockSignalTable()

		tableQuery := tablequery.NewTableQuery(tableDescription.ID(), true, "", 0, defaultLimit)

		iterator, err := dblog.NewIncrementalIterator(
			logger.Log,
			storage,
			tableQuery,
			signalTable,
			postgres.Represent,
			primaryKey,
			nil,
			defaultLimit,
		)

		require.NoError(t, err)

		items, err := iterator.Next(context.TODO())
		require.NoError(t, err)
		require.Equal(t, 1, len(items))

		readIdx := items[0].ColumnValues[1].(int32)
		require.Equal(t, int32(0), readIdx)

		items, err = iterator.Next(context.TODO())
		require.NoError(t, err)
		require.Equal(t, 1, len(items))

		readIdx = items[0].ColumnValues[1].(int32)
		require.Equal(t, int32(1), readIdx)
	}
}

func addIdxToPk(values []string) []string {
	res := make([]string, len(values))

	for i, value := range values {
		res[i] = value + fmt.Sprintf(", %d", i)
	}

	return res
}

func insertQueryValues(tableName string, values []string) string {
	insert := fmt.Sprintf(`INSERT INTO "%s" VALUES (%s)`, tableName, values[0])

	for i := 1; i < len(values); i++ {
		insert += fmt.Sprintf(", (%s)", values[i])
	}

	insert += ";"

	return insert
}

func createTableNameForType(pgType string) string {
	return fmt.Sprintf(`%s_pk_table`, pgType)
}

func createTableWithPkTypeQuery(pgType string) string {
	tableName := createTableNameForType(pgType)

	return fmt.Sprintf(`
		CREATE TABLE "%s" (
		pk %s PRIMARY KEY,
		idx INT);
	`, tableName, pgType)
}
