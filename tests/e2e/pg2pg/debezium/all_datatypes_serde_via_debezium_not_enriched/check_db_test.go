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
    true,
    b'1',
    b'10101111',
    b'10101110',

    -32768,
    -- 1,
    -8388605,
    -- 0,
    1,
    -- 3372036854775807,
    2,

    1.45e-10,
    3.14e-100,

    '1',
    'varchar_example',

    'abcd',
    'varc',
    '2004-10-19 10:23:54+02',
    '2004-10-19 11:23:54+02',
    '00:51:02.746572-08',
    '00:51:02.746572-08',
    interval '1 day 01:00:00',
    decode('CAFEBABE', 'hex'),

    '{"k1": "v1"}',
    '{"k2": "v2"}',
    '<foo>bar</foo>',

    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    point(23.4, -44.5),
    '192.168.100.128/25',
    '[3,7)'::int4range,
    '[3,7)'::int8range,
    numrange(1.9,1.91),
    '[2010-01-02 10:00, 2010-01-02 11:00)',
    '[2010-01-01 01:00:00 -05, 2010-01-01 02:00:00 -08)'::tstzrange,
    daterange('2000-01-10'::date, '2000-01-20'::date, '[]'),

    1.45e-10,
    2,
    'text_example',

    -- ----------------------------------------------------------------------------------------------------------------

    --     DATE_ DATE,
    'January 8, 1999',

    --     TIME_ TIME,
    --     TIME1 TIME(1), -- precision: This is a fractional digits number placed in the seconds’ field. This can be up to six digits. HH:MM:SS.pppppp
    --     TIME6 TIME(6),
    '04:05:06',
    '04:05:06.1',
    '04:05:06.123456',

    --     TIMETZ__ TIME WITH TIME ZONE,
    --     TIMETZ1 TIME(1) WITH TIME ZONE,
    --     TIMETZ6 TIME(6) WITH TIME ZONE,
    '2020-05-26 13:30:25-04',
    '2020-05-26 13:30:25.5-04',
    '2020-05-26 13:30:25.575401-04',

    --     TIMESTAMP1 TIMESTAMP(1),
    --     TIMESTAMP6 TIMESTAMP(6),
    --     TIMESTAMP TIMESTAMP,
    '2004-10-19 10:23:54.9',
    '2004-10-19 10:23:54.987654',
    '2004-10-19 10:23:54',

    --
    --     NUMERIC_ NUMERIC,
    --     NUMERIC_5 NUMERIC(5),
    --     NUMERIC_5_2 NUMERIC(5,2),
    1267650600228229401496703205376,
    12345,
    123.67,

    --     DECIMAL_ DECIMAL,
    --     DECIMAL_5 DECIMAL(5),
    --     DECIMAL_5_2 DECIMAL(5,2),
    123456,
    12345,
    123.67,

    --     MONEY_ MONEY,
    99.98,

    --     HSTORE_ HSTORE,
    'a=>1,b=>2',

    --     INET_ INET,
    '192.168.1.5',

    --     CIDR_ CIDR,
    '10.1/16',

    --     MACADDR_ MACADDR,
    '08:00:2b:01:02:03',

    --     CITEXT_ CITEXT
    'Tom'
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
	receiver := debezium.NewReceiver(nil, nil)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Src.(*pgcommon.PgSource).NoHomo = true
	transfer.Src.(*pgcommon.PgSource).PreSteps.Table = false
	transfer.Src.(*pgcommon.PgSource).PreSteps.PrimaryKey = false
	transfer.Dst.(*pgcommon.PgDestination).MaintainTables = true

	debeziumSerDeTransformer := simple_transformer.NewSimpleTransformer(t, serde.MakeDebeziumSerDeUdfWithoutCheck(emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))
	helpers.Activate(t, transfer)

	//---

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), insertStmt)
	require.NoError(t, err)

	//---

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "basic_types", helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 2))
	require.Equal(t, 2, serde.CountOfProcessedMessage)
}

// Target schema:
// create table if not exists "public"."basic_types" (
//	"i" integer,
//	"bl" boolean,
//	"b" boolean,
//	"b8" bytea,
//	"vb" bytea,
//	"si" smallint,
//	"int" integer,
//	"id" bigint,
//	"oid_" bigint,
//	"real_" double precision,
//	"d" double precision,
//	"c" text,
//	"str" text,
//	"character_" text,
//	"character_varying_" text,
//	"timestamptz_" text,
//	"tst" text,
//	"timetz_" text,
//	"time_with_time_zone_" text,
//	"iv" bigint,
//	"ba" bytea,
//	"j" text,
//	"jb" text,
//	"x" text,
//	"uid" text,
//	"pt" text,
//	"it" text,
//	"int4range_" text,
//	"int8range_" text,
//	"numrange_" text,
//	"tsrange_" text,
//	"tstzrange_" text,
//	"daterange_" text,
//	"f" double precision,
//	"t" text,
//	"date_" integer,
//	"time_" bigint,
//	"time1" integer,
//	"time6" bigint,
//	"timetz__" text,
//	"timetz1" text,
//	"timetz6" text,
//	"timestamp1" bigint,
//	"timestamp6" bigint,
//	"timestamp" bigint,
//	"numeric_" double precision,
//	"numeric_5" text,
//	"numeric_5_2" text,
//	"decimal_" double precision,
//	"decimal_5" text,
//	"decimal_5_2" text,
//	"money_" text,
//	"hstore_" text,
//	"inet_" text,
//	"cidr_" text,
//	"macaddr_" text,
//	"citext_" text,
//	 primary key (i)
//)
