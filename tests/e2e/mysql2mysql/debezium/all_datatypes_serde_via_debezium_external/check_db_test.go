package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/serde"
	simple_transformer "github.com/transferia/transferia/tests/helpers/transformer"
)

var (
	Source = *helpers.RecipeMysqlSource()
	Target = *helpers.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

var insertStmt = `
INSERT INTO customers3 VALUES (
    2,

    0,     -- BOOLEAN
    1,     -- BOOL
    1,     -- BIT(1)
    X'9f', -- BIT(16)

    1,   -- TINYINT
    22,  -- TINYINT DEFAULT 0
    255, -- TINYINT UNSIGNED

    1,   -- TINYINT(1)
    1,   -- TINYINT(1) UNSIGNED

    1000, -- SMALLINT
    100,  -- SMALLINT(5)
    10,   -- SMALLINT UNSIGNED

    1,    -- MEDIUMINT
    11,   -- MEDIUMINT(5)
    111,  -- MEDIUMINT UNSIGNED

    9,     -- INT
    99,    -- INTEGER
    999,   -- INTEGER(5)
    9999,  -- INT UNSIGNED

    8,    -- BIGINT
    88,   -- BIGINT(5)
    888,  -- BIGINT UNSIGNED

    -- REAL

    123.45,   -- REAL
    99999.99, -- REAL(10, 2)

    1.23, -- FLOAT
    1.23, -- FLOAT(53)

    2.34, -- DOUBLE
    2.34, -- DOUBLE PRECISION

    -- CHAR

    'a',   -- CHAR
    'abc', -- CHAR(5)

    'blab', -- VARCHAR(5)

    X'9f', -- BINARY
    X'9f', -- BINARY(5)

    X'9f9f', -- VARBINARY(5)

    X'9f9f9f',     -- TINYBLOB
    'qwerty12345', -- TINYTEXT

    X'ff',               -- BLOB
    'my-text',           -- TEXT
    X'abcd',             -- MEDIUMBLOB
    'my-mediumtext',     -- MEDIUMTEXT
    X'abcd',             -- LONGBLOB
    'my-longtext',       -- LONGTEXT
    '{"k1": "v1"}',      -- JSON
    'x-small',           -- ENUM('x-small', 'small', 'medium', 'large', 'x-large')
    'a',                 -- SET('a', 'b', 'c', 'd')

    -- TEMPORAL DATA TYPES

    1901, -- YEAR
    2155, -- YEAR(4)

    '1999-01-01 00:00:01',        -- TIMESTAMP
    '1999-10-19 10:23:54',        -- TIMESTAMP(0)
    '2004-10-19 10:23:54.1',      -- TIMESTAMP(1)
    '2004-10-19 10:23:54.12',     -- TIMESTAMP(2)
    '2004-10-19 10:23:54.123',    -- TIMESTAMP(3)
    '2004-10-19 10:23:54.1234',   -- TIMESTAMP(4)
    '2004-10-19 10:23:54.12345',  -- TIMESTAMP(5)
    '2004-10-19 10:23:54.123456', -- TIMESTAMP(6)

    -- TEMPORAL TYPES

    '1000-01-01',   -- DATE

    '04:05:06',        -- TIME
    '04:05:06',        -- TIME(0)
    -- '04:05:06.1',      -- TIME(1)
    '04:05:06.12',     -- TIME(2)
    -- '04:05:06.123',    -- TIME(3)
    '04:05:06.1234',   -- TIME(4)
    -- '04:05:06.12345',  -- TIME(5)
    -- '04:05:06.123456', -- TIME(6)

    '2020-01-01 15:10:10',        -- DATETIME
    '2020-01-01 15:10:10',        -- DATETIME(0)
    '2020-01-01 15:10:10.1',      -- DATETIME(1)
    '2020-01-01 15:10:10.12',     -- DATETIME(2)
    '2020-01-01 15:10:10.123',    -- DATETIME(3)
    '2020-01-01 15:10:10.1234',   -- DATETIME(4)
    '2020-01-01 15:10:10.12345',  -- DATETIME(5)
    '2020-01-01 15:10:10.123456', -- DATETIME(6)

    -- DECIMAL TYPES

    1234567890, -- NUMERIC
    12345,      -- NUMERIC(5)
    123.45,     -- NUMERIC(5,2)

    2345678901, -- DECIMAL
    23451,      -- DECIMAL(5)
    231.45      -- DECIMAL(5,2)
);
`

func init() {
	_ = os.Setenv("YC", "1")                                                                            // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	//---

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.SourceType:       "mysql",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	originalTypes := map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo{
		{Namespace: "", Name: "customers3"}: {
			"pk":               {OriginalType: "mysql:int(10) unsigned"},
			"bool1":            {OriginalType: "mysql:tinyint(1)"},
			"bool2":            {OriginalType: "mysql:tinyint(1)"},
			"bit":              {OriginalType: "mysql:bit(1)"},
			"bit16":            {OriginalType: "mysql:bit(16)"},
			"tinyint_":         {OriginalType: "mysql:tinyint(4)"},
			"tinyint_def":      {OriginalType: "mysql:tinyint(4)"},
			"tinyint_u":        {OriginalType: "mysql:tinyint(3) unsigned"},
			"tinyint1":         {OriginalType: "mysql:tinyint(1)"},
			"tinyint1u":        {OriginalType: "mysql:tinyint(1) unsigned"},
			"smallint_":        {OriginalType: "mysql:smallint(6)"},
			"smallint5":        {OriginalType: "mysql:smallint(5)"},
			"smallint_u":       {OriginalType: "mysql:smallint(5) unsigned"},
			"mediumint_":       {OriginalType: "mysql:mediumint(9)"},
			"mediumint5":       {OriginalType: "mysql:mediumint(5)"},
			"mediumint_u":      {OriginalType: "mysql:mediumint(8) unsigned"},
			"int_":             {OriginalType: "mysql:int(11)"},
			"integer_":         {OriginalType: "mysql:int(11)"},
			"integer5":         {OriginalType: "mysql:int(5)"},
			"int_u":            {OriginalType: "mysql:int(10) unsigned"},
			"bigint_":          {OriginalType: "mysql:bigint(20)"},
			"bigint5":          {OriginalType: "mysql:bigint(5)"},
			"bigint_u":         {OriginalType: "mysql:bigint(20) unsigned"},
			"real_":            {OriginalType: "mysql:double"},
			"real_10_2":        {OriginalType: "mysql:double(10,2)"},
			"float_":           {OriginalType: "mysql:float"},
			"float_53":         {OriginalType: "mysql:double"},
			"double_":          {OriginalType: "mysql:double"},
			"double_precision": {OriginalType: "mysql:double"},
			"char_":            {OriginalType: "mysql:char(1)"},
			"char5":            {OriginalType: "mysql:char(5)"},
			"varchar5":         {OriginalType: "mysql:varchar(5)"},
			"binary_":          {OriginalType: "mysql:binary(1)"},
			"binary5":          {OriginalType: "mysql:binary(5)"},
			"varbinary5":       {OriginalType: "mysql:varbinary(5)"},
			"tinyblob_":        {OriginalType: "mysql:tinyblob"},
			"tinytext_":        {OriginalType: "mysql:tinytext"},
			"blob_":            {OriginalType: "mysql:blob"},
			"text_":            {OriginalType: "mysql:text"},
			"mediumblob_":      {OriginalType: "mysql:mediumblob"},
			"mediumtext_":      {OriginalType: "mysql:mediumtext"},
			"longblob_":        {OriginalType: "mysql:longblob"},
			"longtext_":        {OriginalType: "mysql:longtext"},
			"json_":            {OriginalType: "mysql:json"},
			"enum_":            {OriginalType: "mysql:enum('x-small','small','medium','large','x-large')"},
			"set_":             {OriginalType: "mysql:set('a','b','c','d')"},
			"year_":            {OriginalType: "mysql:year(4)"},
			"year4":            {OriginalType: "mysql:year(4)"},
			"timestamp_":       {OriginalType: "mysql:timestamp"},
			"timestamp0":       {OriginalType: "mysql:timestamp"},
			"timestamp1":       {OriginalType: "mysql:timestamp(1)"},
			"timestamp2":       {OriginalType: "mysql:timestamp(2)"},
			"timestamp3":       {OriginalType: "mysql:timestamp(3)"},
			"timestamp4":       {OriginalType: "mysql:timestamp(4)"},
			"timestamp5":       {OriginalType: "mysql:timestamp(5)"},
			"timestamp6":       {OriginalType: "mysql:timestamp(6)"},
			"date_":            {OriginalType: "mysql:date"},
			"time_":            {OriginalType: "mysql:time"},
			"time0":            {OriginalType: "mysql:time"},
			"time1":            {OriginalType: "mysql:time(1)"},
			"time2":            {OriginalType: "mysql:time(2)"},
			"time3":            {OriginalType: "mysql:time(3)"},
			"time4":            {OriginalType: "mysql:time(4)"},
			"time5":            {OriginalType: "mysql:time(5)"},
			"time6":            {OriginalType: "mysql:time(6)"},
			"datetime_":        {OriginalType: "mysql:datetime"},
			"datetime0":        {OriginalType: "mysql:datetime"},
			"datetime1":        {OriginalType: "mysql:datetime(1)"},
			"datetime2":        {OriginalType: "mysql:datetime(2)"},
			"datetime3":        {OriginalType: "mysql:datetime(3)"},
			"datetime4":        {OriginalType: "mysql:datetime(4)"},
			"datetime5":        {OriginalType: "mysql:datetime(5)"},
			"datetime6":        {OriginalType: "mysql:datetime(6)"},
			"NUMERIC_":         {OriginalType: "mysql:decimal(10,0)"},
			"NUMERIC_5":        {OriginalType: "mysql:decimal(5,0)"},
			"NUMERIC_5_2":      {OriginalType: "mysql:decimal(5,2)"},
			"DECIMAL_":         {OriginalType: "mysql:decimal(10,0)"},
			"DECIMAL_5":        {OriginalType: "mysql:decimal(5,0)"},
			"DECIMAL_5_2":      {OriginalType: "mysql:decimal(5,2)"},
		},
	}
	receiver := debezium.NewReceiver(originalTypes, nil)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Src.(*mysql.MysqlSource).PlzNoHomo = true
	transfer.Src.(*mysql.MysqlSource).AllowDecimalAsFloat = true
	debeziumSerDeTransformer := simple_transformer.NewSimpleTransformer(t, serde.MakeDebeziumSerDeUdfWithoutCheck(emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//---

	connParams, err := mysql.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)
	db, err := mysql.Connect(connParams, nil)
	require.NoError(t, err)

	_, err = db.Exec(insertStmt)
	require.NoError(t, err)

	//---

	require.NoError(t, helpers.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "customers3",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
