package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/serde"
	simple_transformer "github.com/transferia/transferia/tests/helpers/transformer"
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
    1,
    -8388605,
    0,
    1,
    3372036854775807,
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
	originalTypes := map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo{
		{Namespace: "public", Name: "basic_types"}: {
			"i":                    {OriginalType: "pg:integer"},
			"bl":                   {OriginalType: "pg:boolean"},
			"b":                    {OriginalType: "pg:bit(1)"},
			"b8":                   {OriginalType: "pg:bit(8)"},
			"vb":                   {OriginalType: "pg:bit varying(8)"},
			"si":                   {OriginalType: "pg:smallint"},
			"ss":                   {OriginalType: "pg:smallint"},
			"int":                  {OriginalType: "pg:integer"},
			"aid":                  {OriginalType: "pg:integer"},
			"id":                   {OriginalType: "pg:bigint"},
			"bid":                  {OriginalType: "pg:bigint"},
			"oid_":                 {OriginalType: "pg:oid"},
			"real_":                {OriginalType: "pg:real"},
			"d":                    {OriginalType: "pg:double precision"},
			"c":                    {OriginalType: "pg:character(1)"},
			"str":                  {OriginalType: "pg:character varying(256)"},
			"character_":           {OriginalType: "pg:character(4)"},
			"character_varying_":   {OriginalType: "pg:character varying(5)"},
			"timestamptz_":         {OriginalType: "pg:timestamp with time zone"},
			"tst":                  {OriginalType: "pg:timestamp with time zone"},
			"timetz_":              {OriginalType: "pg:time with time zone"},
			"time_with_time_zone_": {OriginalType: "pg:time with time zone"},
			"iv":                   {OriginalType: "pg:interval"},
			"ba":                   {OriginalType: "pg:bytea"},
			"j":                    {OriginalType: "pg:json"},
			"jb":                   {OriginalType: "pg:jsonb"},
			"x":                    {OriginalType: "pg:xml"},
			"uid":                  {OriginalType: "pg:uuid"},
			"pt":                   {OriginalType: "pg:point"},
			"it":                   {OriginalType: "pg:inet"},
			"int4range_":           {OriginalType: "pg:int4range"},
			"int8range_":           {OriginalType: "pg:int8range"},
			"numrange_":            {OriginalType: "pg:numrange"},
			"tsrange_":             {OriginalType: "pg:tsrange"},
			"tstzrange_":           {OriginalType: "pg:tstzrange"},
			"daterange_":           {OriginalType: "pg:daterange"},
			"f":                    {OriginalType: "pg:double precision"},
			"t":                    {OriginalType: "pg:text"},
			"date_":                {OriginalType: "pg:date"},
			"time_":                {OriginalType: "pg:time without time zone"},
			"time1":                {OriginalType: "pg:time(1) without time zone"},
			"time6":                {OriginalType: "pg:time(6) without time zone"},
			"timetz__":             {OriginalType: "pg:time with time zone"},
			"timetz1":              {OriginalType: "pg:time with time zone"},
			"timetz6":              {OriginalType: "pg:time with time zone"},
			"timestamp1":           {OriginalType: "pg:timestamp(1) without time zone"},
			"timestamp6":           {OriginalType: "pg:timestamp(6) without time zone"},
			"timestamp":            {OriginalType: "pg:timestamp without time zone"},
			"numeric_":             {OriginalType: "pg:numeric"},
			"numeric_5":            {OriginalType: "pg:numeric"},
			"numeric_5_2":          {OriginalType: "pg:numeric"},
			"decimal_":             {OriginalType: "pg:numeric"},
			"decimal_5":            {OriginalType: "pg:numeric"},
			"decimal_5_2":          {OriginalType: "pg:numeric"},
			"money_":               {OriginalType: "pg:money"},
			"hstore_":              {OriginalType: "pg:hstore"},
			"inet_":                {OriginalType: "pg:inet"},
			"cidr_":                {OriginalType: "pg:cidr"},
			"macaddr_":             {OriginalType: "pg:macaddr"},
			"citext_":              {OriginalType: "pg:citext"},
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
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
	require.Equal(t, 2, serde.CountOfProcessedMessage)
}
