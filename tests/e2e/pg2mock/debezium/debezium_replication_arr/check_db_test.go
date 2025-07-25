package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/library/go/test/yatest"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	"github.com/transferia/transferia/pkg/debezium/testutil"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("init_source"))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------

var insertStmt = `
INSERT INTO public.basic_types VALUES (
    1,

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

func ReadTextFiles(paths []string, out []*string) error {
	for index, path := range paths {
		valArr, err := os.ReadFile(yatest.SourcePath(path))
		if err != nil {
			return xerrors.Errorf("unable to read file %s: %w", path, err)
		}
		val := string(valArr)
		*out[index] = val
	}
	return nil
}

func TestReplication(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))

	//------------------------------------------------------------------------------

	var canonizedDebeziumInsertKey = ``
	var canonizedDebeziumInsertVal = ``

	err := ReadTextFiles(
		[]string{
			"transfer_manager/go/tests/e2e/pg2mock/debezium/debezium_replication_arr/testdata/debezium_msg_0_key.txt",
			"transfer_manager/go/tests/e2e/pg2mock/debezium/debezium_replication_arr/testdata/debezium_msg_0_val.txt",
		},
		[]*string{
			&canonizedDebeziumInsertKey,
			&canonizedDebeziumInsertVal,
		},
	)
	require.NoError(t, err)

	fmt.Printf("canonizedDebeziumInsertKey=%s\n", canonizedDebeziumInsertKey)
	fmt.Printf("canonizedDebeziumInsertVal=%s\n", canonizedDebeziumInsertVal)

	//------------------------------------------------------------------------------
	// start replication

	sinker := &helpers.MockSink{}
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer := helpers.MakeTransfer("fake", &Source, &target, abstract.TransferTypeSnapshotAndIncrement)

	mutex := sync.Mutex{}
	var changeItems []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		found := false
		for _, el := range input {
			if el.Table == "basic_types" {
				found = true
			}
		}
		if !found {
			return nil
		}
		//---
		mutex.Lock()
		defer mutex.Unlock()

		for _, el := range input {
			if el.Table != "basic_types" {
				continue
			}
			changeItems = append(changeItems, el)
		}

		return nil
	}

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//-----------------------------------------------------------------------------------------------------------------
	// execute SQL statements

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), insertStmt)
	require.NoError(t, err)

	for {
		time.Sleep(time.Second)

		mutex.Lock()
		if len(changeItems) == 5 {
			break
		}
		mutex.Unlock()
	}

	require.Equal(t, changeItems[0].Kind, abstract.InitShardedTableLoad)
	require.Equal(t, changeItems[1].Kind, abstract.InitTableLoad)
	require.Equal(t, changeItems[2].Kind, abstract.DoneTableLoad)
	require.Equal(t, changeItems[3].Kind, abstract.DoneShardedTableLoad)
	require.Equal(t, changeItems[4].Kind, abstract.InsertKind)

	for i := range changeItems {
		fmt.Printf("changeItem dump: %s\n", changeItems[i].ToJSONString())
	}

	//-----------------------------------------------------------------------------------------------------------------

	canonizeTypes(t, &changeItems[4])

	testSuite := []debeziumcommon.ChangeItemCanon{
		{
			ChangeItem: &changeItems[4],
			DebeziumEvents: []debeziumcommon.KeyValue{{
				DebeziumKey: canonizedDebeziumInsertKey,
				DebeziumVal: &canonizedDebeziumInsertVal,
			}},
		},
	}

	testSuite = testutil.FixTestSuite(t, testSuite, "fullfillment", "pguser", "pg")

	for _, testCase := range testSuite {
		testutil.CheckCanonizedDebeziumEvent(t, testCase.ChangeItem, "fullfillment", "pguser", "pg", false, testCase.DebeziumEvents)
	}

	for i := range testSuite {
		testSuite[i].ChangeItem = helpers.UnmarshalChangeItemStr(t, testSuite[i].ChangeItem.ToJSONString())
	}

	for _, testCase := range testSuite {
		testutil.CheckCanonizedDebeziumEvent(t, testCase.ChangeItem, "fullfillment", "pguser", "pg", false, testCase.DebeziumEvents)
	}
}

func canonizeTypes(t *testing.T, item *abstract.ChangeItem) {
	colNameToOriginalType := make(map[string]string)
	for _, el := range item.TableSchema.Columns() {
		colNameToOriginalType[el.ColumnName] = el.OriginalType
	}
	for i := range item.ColumnNames {
		currColName := item.ColumnNames[i]
		currColVal := item.ColumnValues[i]
		currOriginalType, ok := colNameToOriginalType[currColName]
		require.True(t, ok)
		fieldType := fmt.Sprintf("%T", currColVal)
		colNameToOriginalType[currColName] = fmt.Sprintf(`%s:%s`, currOriginalType, fieldType)
		if fieldType == "[]interface {}" && len(currColVal.([]interface{})) != 0 {
			currType2 := fmt.Sprintf(`%s:%s`, currOriginalType, fmt.Sprintf("%T", currColVal.([]interface{})[0]))
			colNameToOriginalType["    ELEM:"+currColName] = currType2
		}
	}
	canon.SaveJSON(t, colNameToOriginalType)
}
