package datatypes

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	server "github.com/transferia/transferia/pkg/abstract/model"
	mysqlSource "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	Source            = helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{"test_table"})
	ytTestPath        = "//home/cdc/test/mysql2yt_all_types"
	Target            = yt_helpers.RecipeYtTarget(ytTestPath)
	insertRowsRequest = strings.ReplaceAll(`
                        INSERT INTO test_table
                        (%stinyint%s, %stinyint_def%s, %stinyint_u%s, %stinyint_z%s, %ssmallint%s, %ssmallint_u%s, %ssmallint_z%s, %smediumint%s, %smediumint_u%s, %smediumint_z%s, %sint%s       , %sint_u%s    , %sint_z%s    , %sbigint%s            , %sbigint_u%s        , %sbigint_z%s       ,  %sbool%s, %sfixed%s,%sdecimal_10_2%s  ,%sdecimal_65_30%s, %sdecimal_65_0%s, %sdec%s   , %snumeric%s      , %sfloat%s        , %sfloat_z%s  , %sfloat_53%s, %sreal%s    , %sdouble%s                 , %sdouble_precision%s, %sbit%s, %sbit_5%s, %sdate%s       , %sdatetime%s            , %sdatetime_6%s          ,  %stimestamp%s          , %stimestamp_2%s      ,  %stime%s      , %stime_2%s     , %syear%s, %schar%s, %svarchar%s, %svarchar_def%s, %sbinary%s, %svarbinary%s, %stinyblob%s, %sblob%s, %smediumblob%s, %slongblob%s, %stinytext%s, %stext%s, %smediumtext%s, %slongtext%s, %senum%s , %sset%s, %sjson%s,    %sid%s                        )
                        VALUES
                        (-128       , -128           , 0            , 0            , -32768      , 0             , 0             , -8388608     , 0              , 0              , -2147483648   , 0            , 0            , -9223372036854775808  , 0                   , 0                  , 0        , '4'      , '3.50'           , '45.67'         , '4567'          , '4'       , '3.50'           , 1.02345678E+07   , '3.50'       , '3.50'      , '3.50'      , -1.7976931348623157E+308   , '3.50'              , 0      , '01'     , '1970-01-01'   , '1970-01-01 00:00:00'   , '1970-01-01 00:00:00'   , '1970-01-01 03:00:01'   , '1970-01-01 03:00:01', '-838:59:59'   , '-838:59:59'   , '1901'  , 0       , ''         , ''             , ''        , ''           , ''          , ''      , ''            , ''          , ''          , ''      , ''            , ''          , '1'      , '1'    , '{"a":"b"}',                              1)

                        ,
                        (127        , 127            , 255          , 127          , 32767       , 65535         , 32767         , 8388607      , 16777215       , 8388607       , 2147483647    , 4294967295   , 2147483647   , 9223372036854774784    , 18446744073709549568, 9223372036854774784, 1        , '3.50'   , '12345678.1'     , '4567.89'       , 456789          , '12345678', '12345678.1'     , 3.4028234E+5     , '12345678.1' ,'12345678.1' , '12345678.1', -2.2250738585072014E-308   , 0                   , 1      , 31       , '2021-01-19'   , '2099-12-31 23:59:59'   , '2099-12-31 23:59:59'   , '2038-01-19 03:14:07'   , '2038-01-19 03:14:07', '838:59:59'    , '838:59:59'    , '2155'  , 255     , NULL       , NULL           , NULL      , NULL         , NULL        , NULL    , NULL          , NULL        , NULL        , NULL    , NULL          , NULL        , '3'      , '3'    , '{"a":"b"  , "c":1  , "d":{}  , "e":[]}', 2)
                        ;
                    `, "%s", "`")

	Row1 = TestTableRow{
		TinyInt:           -128,
		TinyIntDefault:    -128,
		TinyIntUnsigned:   0,
		TinyIntZezo:       0,
		SmallInt:          -32768,
		SmallIntUnsigned:  0,
		SmallIntZero:      0,
		MediumInt:         -8388608,
		MediumIntUnsigned: 0,
		MediumIntZero:     0,
		Int:               -2147483648,
		IntUnsigned:       0,
		IntZero:           0,
		BigInt:            -9223372036854775808,
		BigIntUnsigned:    0,
		BigIntZero:        0,
		Bool:              0,
		Fixed:             4,
		Decimal10_2:       3.50,
		Decimal65_30:      45.67,
		Decimal65_0:       4567,
		Dec:               4,
		Numeric:           3.50,
		Float:             1.0234568e+07,
		FloatZero:         3.50,
		Float53:           3.50,
		Real:              3.50,
		Double:            -1.7976931348623157e+308,
		DoublePrecision:   3.50,
		Bit:               string([]byte{0}),
		Bit5:              string([]byte{0x1F}),
		Date:              0,
		DateTime:          0,
		DateTime6:         0,
		Timestamp:         10801000000,
		Timestamp2:        10801000000,
		Time:              "-838:59:59",
		Time2:             "-838:59:59",
		Year:              "1901",
		Char:              "0",
		Varchar:           "",
		VarcharDefault:    "",
		Binary:            []byte(""),
		VarBinary:         []byte(""),
		TinyBlob:          []byte(""),
		Blob:              []byte(""),
		MediumBlob:        []byte(""),
		LongBlob:          []byte(""),
		TinyText:          "",
		Text:              "",
		MediumText:        "",
		LongText:          "",
		Enum:              "1",
		Set:               "1",
		JSON:              map[string]interface{}{"a": "b"},
		ID:                1,
	}
	Row2 = TestTableRow{
		TinyInt:           127,
		TinyIntDefault:    127,
		TinyIntUnsigned:   255,
		TinyIntZezo:       127,
		SmallInt:          32767,
		SmallIntUnsigned:  65535,
		SmallIntZero:      32767,
		MediumInt:         8388607,
		MediumIntUnsigned: 16777215,
		MediumIntZero:     8388607,
		Int:               2147483647,
		IntUnsigned:       4294967295,
		IntZero:           2147483647,
		BigInt:            9223372036854774784,
		BigIntUnsigned:    18446744073709549568,
		BigIntZero:        9223372036854774784,
		Bool:              1,
		Fixed:             4,
		Decimal10_2:       12345678.1,
		Decimal65_30:      4567.89,
		Decimal65_0:       456789,
		Dec:               12345678,
		Numeric:           12345678.1,
		Float:             3.4028234e+5,
		FloatZero:         12345678,
		Float53:           12345678.1,
		Real:              12345678.1,
		Double:            -2.2250738585072014e-308,
		DoublePrecision:   0,
		Bit:               string([]byte{1}),
		Bit5:              string([]byte{0x1F}),
		Date:              18646,
		DateTime:          4102444799000000,
		DateTime6:         4102444799000000,
		Timestamp:         2147483647000000,
		Timestamp2:        2147483647000000,
		Time:              "838:59:59",
		Time2:             "838:59:59",
		Year:              "2155",
		Char:              "255",
		Varchar:           "",
		VarcharDefault:    "",
		Binary:            nil,
		VarBinary:         nil,
		TinyBlob:          nil,
		Blob:              nil,
		MediumBlob:        nil,
		LongBlob:          nil,
		TinyText:          "",
		Text:              "",
		MediumText:        "",
		LongText:          "",
		Enum:              "3",
		Set:               "3",
		JSON: map[string]interface{}{
			"a": "b",
			"c": "1",
			"d": map[string]interface{}{},
			"e": []interface{}{},
		},
		ID: 2,
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Source.AllowDecimalAsFloat = true
}

func TestReplication(t *testing.T) {
	ctx := context.Background()

	transfer := server.Transfer{
		ID:  "mysql2yt",
		Src: Source,
		Dst: Target,
	}

	fakeClient := coordinator.NewStatefulFakeClient()
	syncBinlogPosition := func() {
		err := mysqlSource.SyncBinlogPosition(Source, transfer.ID, fakeClient)
		require.NoError(t, err)
	}
	syncBinlogPosition()

	ytEnv := yt_helpers.NewEnvWithNode(t, ytTestPath)

	// check
	conn, err := mysqlDriver.NewConnector(makeMysqlConfig(Source))
	require.NoError(t, err)
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logger.Log.Warn("unable to close mysql db", log.Error(err))
		}
	}(db)

	ytPath := ypath.Path(fmt.Sprintf("%v/source_test_table", ytTestPath))

	readAllRowsF := func() []TestTableRow {
		return readAllRows(t, ytEnv.YT, ctx, ytPath)
	}
	checkDataWithDelay := func(expected []TestTableRow, delay time.Duration) {
		checkData(t, readAllRowsF, expected, delay)
	}
	checkDataF := func(expected []TestTableRow) {
		checkDataWithDelay(expected, time.Second)
	}

	worker1 := startWorker(transfer, fakeClient)
	defer stopWorker(worker1)

	CheckInsert(t, db, checkDataF)
	CheckUpdate(t, db, checkDataF)
	//CheckDelete(t, db, checkDataF)
}

func CheckInsert(t *testing.T, db *sql.DB, checkData func([]TestTableRow)) {
	_, err := db.Exec(insertRowsRequest)
	require.NoError(t, err)
	checkData([]TestTableRow{Row1, Row2})
}

func CheckUpdate(t *testing.T, db *sql.DB, checkData func([]TestTableRow)) {
	_, err := db.Exec("UPDATE test_table SET `tinyint` = 126 WHERE `id` = 1")
	require.NoError(t, err)
	Row1.TinyInt = 126
	checkData([]TestTableRow{Row1, Row2})
}

func CheckDelete(t *testing.T, db *sql.DB, checkData func([]TestTableRow)) {
	_, err := db.Exec("DELETE FROM test_table WHERE id = 2")
	require.NoError(t, err)
	checkData([]TestTableRow{Row1})
}

type TestTableRow struct {
	TinyInt         int8  `yson:"tinyint"`
	TinyIntDefault  int8  `yson:"tinyint_def"`
	TinyIntUnsigned uint8 `yson:"tinyint_u"`
	TinyIntZezo     int8  `yson:"tinyint_z"`

	SmallInt         int16  `yson:"smallint"`
	SmallIntUnsigned uint16 `yson:"smallint_u"`
	SmallIntZero     int16  `yson:"smallint_z"` // TODO FILLZERO is also unsigned TM-2943

	MediumInt         int32  `yson:"mediumint"`
	MediumIntUnsigned uint32 `yson:"mediumint_u"`
	MediumIntZero     int32  `yson:"mediumint_z"`

	Int         int32  `yson:"int"`
	IntUnsigned uint32 `yson:"int_u"`
	IntZero     int32  `yson:"int_z"`

	BigInt         int64  `yson:"bigint"`
	BigIntUnsigned uint64 `yson:"bigint_u"`
	BigIntZero     int64  `yson:"bigint_z"`

	Bool int8 `yson:"bool"`

	Decimal10_2     float64 `yson:"decimal_10_2"`
	Decimal65_30    float64 `yson:"decimal_65_30"`
	Decimal65_0     float64 `yson:"decimal_65_0"`
	Dec             float64 `yson:"dec"`
	Numeric         float64 `yson:"numeric"`
	Fixed           float64 `yson:"fixed"`
	Float           float64 `yson:"float"`
	FloatZero       float64 `yson:"float_z"`
	Float53         float64 `yson:"float_53"`
	Real            float64 `yson:"real"`
	Double          float64 `yson:"double"`
	DoublePrecision float64 `yson:"double_precision"`

	Bit  string `yson:"bit"`
	Bit5 string `yson:"bit_5"`

	Date       schema.Date      `yson:"date"`
	DateTime   schema.Timestamp `yson:"datetime"`
	DateTime6  schema.Timestamp `yson:"datetime_6"`
	Timestamp  schema.Timestamp `yson:"timestamp"`
	Timestamp2 schema.Timestamp `yson:"timestamp_2"`

	Time  string `yson:"time"`
	Time2 string `yson:"time_2"`
	Year  string `yson:"year"`

	Char           string `yson:"char"`
	Varchar        string `yson:"varchar"`
	VarcharDefault string `yson:"varchar_def"`

	Binary     []byte `yson:"binary"`
	VarBinary  []byte `yson:"varbinary"`
	TinyBlob   []byte `yson:"tinyblob"`
	Blob       []byte `yson:"blob"`
	MediumBlob []byte `yson:"mediumblob"`
	LongBlob   []byte `yson:"longblob"`

	TinyText   string `yson:"tinytext"`
	Text       string `yson:"text"`
	MediumText string `yson:"mediumtext"`
	LongText   string `yson:"longtext"`

	Enum string `yson:"enum"`
	Set  string `yson:"set"`

	JSON interface{} `yson:"json"`

	ID int `yson:"id"`
}

func checkData(t *testing.T, readAllRows func() []TestTableRow, expected []TestTableRow, delay time.Duration) {
	const (
		retryDelay    = time.Second
		attemptsCount = 10
	)

	time.Sleep(delay)

	for i := 0; i < attemptsCount-1; i++ {
		actual := readAllRows()
		if reflect.DeepEqual(expected, actual) {
			return
		} else {
			logger.Log.Info("values are not equal, waiting...")
			time.Sleep(retryDelay)
		}
	}

	require.Equal(t, expected, readAllRows())
}

func readAllRows(t *testing.T, ytClient yt.Client, ctx context.Context, ytPath ypath.Path) []TestTableRow {
	exists, err := ytClient.NodeExists(ctx, ytPath, &yt.NodeExistsOptions{})
	require.NoError(t, err)
	if !exists {
		return []TestTableRow{}
	}

	var scheme schema.Schema
	if err := ytClient.GetNode(ctx, ytPath.Attr("schema"), &scheme, nil); err != nil {
		return []TestTableRow{}
	}
	logger.Log.Infof("Schema: %v", scheme.Columns)

	reader, err := ytClient.ReadTable(ctx, ytPath, &yt.ReadTableOptions{})
	require.NoError(t, err)
	defer func(reader yt.TableReader) {
		err := reader.Close()
		if err != nil {
			logger.Log.Warn("unable to close yt reader", log.Error(err))
		}
	}(reader)

	rows := make([]TestTableRow, 0)
	for reader.Next() {
		var row TestTableRow
		err = reader.Scan(&row)
		require.NoError(t, err)
		rows = append(rows, row)
	}
	return rows
}

func startWorker(transfer server.Transfer, cp coordinator.Coordinator) *local.LocalWorker {
	w := local.NewLocalWorker(cp, &transfer, helpers.EmptyRegistry(), logger.Log)
	w.Start()
	return w
}

func stopWorker(worker *local.LocalWorker) {
	err := worker.Stop()
	if err != nil {
		logger.Log.Infof("unable to close worker %v", worker.Runtime())
	}
}

func makeMysqlConfig(mysqlSrc *mysqlSource.MysqlSource) *mysqlDriver.Config {
	cfg := mysqlDriver.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", mysqlSrc.Host, mysqlSrc.Port)
	cfg.User = mysqlSrc.User
	cfg.Passwd = string(mysqlSrc.Password)
	cfg.DBName = mysqlSrc.Database
	cfg.Net = "tcp"
	return cfg
}
