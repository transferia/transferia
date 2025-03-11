package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	simple_transformer "github.com/transferia/transferia/tests/helpers/transformer"
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
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		helpers.LabeledPort{Label: "PG target", Port: Target.Port},
	))

	//---

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Src.(*pgcommon.PgSource).NoHomo = true
	serdeTransformer := simple_transformer.NewSimpleTransformer(t, serdeUdf, anyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(serdeTransformer))
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
}
