package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
	"github.com/transferria/transferria/tests/helpers/serde"
	simple_transformer "github.com/transferria/transferria/tests/helpers/transformer"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

var insertStmt = `
INSERT INTO public.basic_types VALUES (
    2,

    -- -----------------------------------------------------------------------------------------------------------------

    '{true,true}', -- ARR_bl  boolean[],
    -- '{1,1}'    -- ARR_b   bit(1)[],
        -- [io.debezium.relational.TableSchemaBuilder]
        -- org.apache.kafka.connect.errors.DataException: Invalid Java object for schema with type BOOLEAN: class java.util.ArrayList for field: "arr_b"

    -- ARR_b8  bit(8)[],
    -- ARR_vb  varbit(8)[],

    '{1,2}', -- ARR_si   smallint[],
    '{1,2}', -- ARR_int  integer[],
    '{1,2}', -- ARR_id   bigint[],
    '{1,2}', -- ARR_oid_ oid[],

    '{1.45e-10,1.45e-10}',   -- ARR_real_ real[],
    '{3.14e-100,3.14e-100}', -- ARR_d   double precision[],

    '{"1", "1"}', -- ARR_c   char[],
    '{"varchar_example", "varchar_example"}', -- ARR_str varchar(256)[],

    '{"abcd","abcd"}', -- ARR_CHARACTER_ CHARACTER(4)[],
    '{"varc","varc"}', -- ARR_CHARACTER_VARYING_ CHARACTER VARYING(5)[],
    '{"2004-10-19 10:23:54+02","2004-10-19 10:23:54+02"}', -- ARR_TIMESTAMPTZ_ TIMESTAMPTZ[], -- timestamptz is accepted as an abbreviation for timestamp with time zone; this is a PostgreSQL extension
    '{"2004-10-19 11:23:54+02","2004-10-19 11:23:54+02"}', -- ARR_tst TIMESTAMP WITH TIME ZONE[],
    '{"00:51:02.746572-08","00:51:02.746572-08"}',         -- ARR_TIMETZ_ TIMETZ[],
    '{"00:51:02.746572-08","00:51:02.746572-08"}',         -- ARR_TIME_WITH_TIME_ZONE_ TIME WITH TIME ZONE[],

    '{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}', -- ARR_uid uuid[],
    '{"192.168.100.128/25","192.168.100.128/25"}', -- ARR_it  inet[],


    '{"1.45e-10","1.45e-10"}',         -- ARR_f   float[],
    '{1,1}',                           -- ARR_i   int[],
    '{"text_example","text_example"}', -- ARR_t   text[],

    '{"January 8, 1999", "January 8, 1999"}', -- DATE_ DATE,

    '{"04:05:06", "04:05:06"}',               -- TIME_ TIME,
    '{"04:05:06.1", "04:05:06.1"}',           -- TIME1 TIME(1),
    '{"04:05:06.123456", "04:05:06.123456"}', -- TIME6 TIME(6),

    '{"2020-05-26 13:30:25-04", "2020-05-26 13:30:25-04"}',               -- TIMETZ__ TIME WITH TIME ZONE,
    '{"2020-05-26 13:30:25.5-04", "2020-05-26 13:30:25.5-04"}',           -- TIMETZ1 TIME(1) WITH TIME ZONE,
    '{"2020-05-26 13:30:25.575401-04", "2020-05-26 13:30:25.575401-04"}', -- TIMETZ6 TIME(6) WITH TIME ZONE,

    '{"2004-10-19 10:23:54.9", "2004-10-19 10:23:54.9"}',           -- TIMESTAMP1 TIMESTAMP(1),
    '{"2004-10-19 10:23:54.987654", "2004-10-19 10:23:54.987654"}', -- TIMESTAMP6 TIMESTAMP(6),
    '{"2004-10-19 10:23:54", "2004-10-19 10:23:54"}',               -- TIMESTAMP TIMESTAMP,

    '{"1267650600228229401496703205376","12676506002282294.01496703205376"}', -- NUMERIC_ NUMERIC,
    '{"12345","12345"}',                                                      -- NUMERIC_5 NUMERIC(5),
    '{"123.67","123.67"}',                                                    -- NUMERIC_5_2 NUMERIC(5,2),

    '{"123456","123456"}',                                                   -- DECIMAL_ DECIMAL,
    '{"12345","12345"}',                                                     -- DECIMAL_5 DECIMAL(5),
    '{"123.67","123.67"}'                                                    -- DECIMAL_5_2 DECIMAL(5,2),

--     '{"a=>1,b=>2","a=>1,b=>2"}',                 -- HSTORE_ HSTORE,
--     '{"192.168.1.5", "192.168.1.5"}',            -- INET_ INET,
--     '{"10.1/16","10.1/16"}',                     -- CIDR_ CIDR,
--     '{"08:00:2b:01:02:03","08:00:2b:01:02:03"}', -- MACADDR_ MACADDR,
--     '{"Tom","Tom"}'                              -- CITEXT_ CITEXT
);
`

func init() {
	_ = os.Setenv("YC", "1")                                                                            // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		helpers.LabeledPort{Label: "PG target", Port: Target.Port},
	))

	//---

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	originalTypes := map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo{
		{Namespace: "public", Name: "basic_types"}: {
			"i":                        {OriginalType: "pg:integer"},
			"arr_bl":                   {OriginalType: "pg:boolean[]"},
			"arr_si":                   {OriginalType: "pg:smallint[]"},
			"arr_int":                  {OriginalType: "pg:integer[]"},
			"arr_id":                   {OriginalType: "pg:bigint[]"},
			"arr_oid_":                 {OriginalType: "pg:oid[]"},
			"arr_real_":                {OriginalType: "pg:real[]"},
			"arr_d":                    {OriginalType: "pg:double precision[]"},
			"arr_c":                    {OriginalType: "pg:character(1)[]"},
			"arr_str":                  {OriginalType: "pg:character varying(256)[]"},
			"arr_character_":           {OriginalType: "pg:character(4)[]"},
			"arr_character_varying_":   {OriginalType: "pg:character varying(5)[]"},
			"arr_timestamptz_":         {OriginalType: "pg:timestamp with time zone[]"},
			"arr_tst":                  {OriginalType: "pg:timestamp with time zone[]"},
			"arr_timetz_":              {OriginalType: "pg:time with time zone[]"},
			"arr_time_with_time_zone_": {OriginalType: "pg:time with time zone[]"},
			"arr_uid":                  {OriginalType: "pg:uuid[]"},
			"arr_it":                   {OriginalType: "pg:inet[]"},
			"arr_f":                    {OriginalType: "pg:double precision[]"},
			"arr_i":                    {OriginalType: "pg:integer[]"},
			"arr_t":                    {OriginalType: "pg:text[]"},
			"arr_date_":                {OriginalType: "pg:date[]"},
			"arr_time_":                {OriginalType: "pg:time without time zone[]"},
			"arr_time1":                {OriginalType: "pg:time(1) without time zone[]"},
			"arr_time6":                {OriginalType: "pg:time(6) without time zone[]"},
			"arr_timetz__":             {OriginalType: "pg:time with time zone[]"},
			"arr_timetz1":              {OriginalType: "pg:time(1) with time zone[]"},
			"arr_timetz6":              {OriginalType: "pg:time(6) with time zone[]"},
			"arr_timestamp1":           {OriginalType: "pg:timestamp(1) without time zone[]"},
			"arr_timestamp6":           {OriginalType: "pg:timestamp(6) without time zone[]"},
			"arr_timestamp":            {OriginalType: "pg:timestamp without time zone[]"},
			"arr_numeric_":             {OriginalType: "pg:numeric[]"},
			"arr_numeric_5":            {OriginalType: "pg:numeric(5,0)[]"},
			"arr_numeric_5_2":          {OriginalType: "pg:numeric(5,2)[]"},
			"arr_decimal_":             {OriginalType: "pg:numeric[]"},
			"arr_decimal_5":            {OriginalType: "pg:numeric(5,0)[]"},
			"arr_decimal_5_2":          {OriginalType: "pg:numeric(5,2)[]"},
		},
	}
	receiver := debezium.NewReceiver(originalTypes, nil)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Src.(*pgcommon.PgSource).NoHomo = true

	debeziumSerDeTransformer := simple_transformer.NewSimpleTransformer(t, serde.MakeDebeziumSerDeUdfWithCheck(emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//---

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), insertStmt)
	require.NoError(t, err)

	//---

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "basic_types", helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 2))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithPriorityComparators(helpers.PgDebeziumIgnoreTemporalAccuracyForArraysComparator)))
	require.Equal(t, 2, serde.CountOfProcessedMessage)
}
