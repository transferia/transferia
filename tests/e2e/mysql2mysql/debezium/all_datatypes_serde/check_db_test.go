package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers"
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

//---------------------------------------------------------------------------------------------------------------------

func serdeUdf(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
	newChangeItems := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)
	for i := range items {
		if items[i].IsSystemTable() {
			continue
		}
		currJSON := items[i].ToJSONString()
		fmt.Printf("changeItem dump:%s\n", currJSON)
		outChangeItem, err := abstract.UnmarshalChangeItem([]byte(currJSON))
		if err != nil {
			errors = append(errors, abstract.TransformerError{
				Input: items[i],
				Error: err,
			})
		} else {
			newChangeItems = append(newChangeItems, *outChangeItem)
		}
	}
	return abstract.TransformerResult{
		Transformed: newChangeItems,
		Errors:      errors,
	}
}

func anyTablesUdf(table abstract.TableID, schema abstract.TableColumns) bool {
	return true
}

//---------------------------------------------------------------------------------------------------------------------

func TestSnapshotAndIncrement(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Src.(*mysql.MysqlSource).PlzNoHomo = true
	transfer.Src.(*mysql.MysqlSource).AllowDecimalAsFloat = true
	serdeTransformer := simple_transformer.NewSimpleTransformer(t, serdeUdf, anyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(serdeTransformer))
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
