//go:build !disable_mysql_provider

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

//--------------------------------------------------------------------------------------------------------------------

const (
	_      = iota // ignore first value by assigning to blank identifier
	KB int = 1 << (10 * iota)
	MB
	GB
	TB
)

//---------------------------------------------------------------------------------------------------------------------

type MockLoggerCounters struct {
	ErrorfCounter  int
	ExpectedErrorf int
}

type MockLogger struct {
	prevLog  *log.Logger
	counters *MockLoggerCounters
}

func (l *MockLogger) SetExpectedErrorf(expectedErrorf int) {
	l.counters.ExpectedErrorf = expectedErrorf
}

func (l MockLogger) Trace(_ string, _ ...log.Field) {}
func (l MockLogger) Debug(_ string, _ ...log.Field) {}
func (l MockLogger) Info(_ string, _ ...log.Field)  {}
func (l MockLogger) Warn(_ string, _ ...log.Field)  {}
func (l MockLogger) Error(_ string, _ ...log.Field) {}
func (l MockLogger) Fatal(_ string, _ ...log.Field) {}

func (l MockLogger) Tracef(_ string, _ ...interface{}) {}
func (l MockLogger) Debugf(_ string, _ ...interface{}) {}
func (l MockLogger) Infof(_ string, _ ...interface{})  {}
func (l MockLogger) Warnf(_ string, _ ...interface{})  {}
func (l MockLogger) Errorf(_ string, _ ...interface{}) { l.counters.ErrorfCounter++ }
func (l MockLogger) Fatalf(_ string, _ ...interface{}) {}

func (l MockLogger) Fmt() log.Fmt                 { return MockLogger{} }
func (l MockLogger) Structured() log.Structured   { return MockLogger{} }
func (l MockLogger) Logger() log.Logger           { return MockLogger{} }
func (l MockLogger) WithName(_ string) log.Logger { return MockLogger{} }

func (l MockLogger) Check(t *testing.T) {
	require.Equal(t, l.counters.ErrorfCounter, l.counters.ExpectedErrorf)
}

func (l *MockLogger) SetMockLogger() {
	l.prevLog = &logger.Log
	logger.Log = *l
}

func (l *MockLogger) ResetLoggerBack() {
	if l.prevLog != nil {
		logger.Log = *l.prevLog
		l.prevLog = nil
	}
}

//---------------------------------------------------------------------------------------------------------------------

type sqlMockWrapper struct {
	mock sqlmock.Sqlmock

	db   *sql.DB
	conn *sql.Conn
	tx   *sql.Tx
}

func (m *sqlMockWrapper) PrepareTx() error {
	return nil
}

func (m *sqlMockWrapper) Close() {
	if m.tx != nil {
		_ = m.tx.Commit()
	}
	if m.conn != nil {
		_ = m.conn.Close()
	}
	_ = m.db.Close()
}

func makeTxMock() (*sqlMockWrapper, error) {
	ctx := context.Background()

	mockObj := new(sqlMockWrapper)

	var err error
	mockObj.db, mockObj.mock, err = sqlmock.New()
	if err != nil {
		return nil, err
	}

	mockObj.mock.ExpectBegin()

	mockObj.conn, err = mockObj.db.Conn(ctx)
	if err != nil {
		_ = mockObj.db.Close()
		return nil, err
	}

	mockObj.tx, err = mockObj.conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		err := mockObj.conn.Close()
		_ = mockObj.db.Close()
		return nil, err
	}
	return mockObj, nil
}

//--------------------------------------------------------------------------------------------------------------------

type SQLRowsMock struct {
	ExpectedCallsNext int
	Index             int
	ColTypes          []*sql.ColumnType
	err               *string
	rowsFunc          func() []interface{}
}

func (m SQLRowsMock) ColumnTypes() ([]*sql.ColumnType, error) {
	if m.err == nil {
		return m.ColTypes, nil
	} else {
		return m.ColTypes, xerrors.Errorf("%s", *m.err)
	}
}

func (m *SQLRowsMock) Next() bool {
	result := m.Index != m.ExpectedCallsNext
	m.Index++
	return result
}

func (m *SQLRowsMock) Scan(toScan ...interface{}) error {
	if m.rowsFunc != nil {
		rows := m.rowsFunc()
		copy(toScan, rows)
	}
	return nil
}

//--------------------------------------------------------------------------------------------------------------------

func makeSQLColumnType(colName string, scanType reflect.Type) *sql.ColumnType {
	var colType sql.ColumnType

	fieldName := reflect.ValueOf(&colType).Elem().FieldByName("name")
	fieldName = reflect.NewAt(fieldName.Type(), unsafe.Pointer(fieldName.UnsafeAddr())).Elem()
	fieldName.SetString(colName)

	fieldScanType := reflect.ValueOf(&colType).Elem().FieldByName("scanType")
	fieldScanType = reflect.NewAt(fieldScanType.Type(), unsafe.Pointer(fieldScanType.UnsafeAddr())).Elem()
	fieldScanType.Set(reflect.ValueOf(scanType))

	return &colType
}

func Test_makeMapColNameToColTypeName(t *testing.T) {
	mockWrapper, err := makeTxMock()
	require.NoError(t, err)
	defer mockWrapper.Close()

	rows := sqlmock.NewRows([]string{"column_name", "column_type"}).
		AddRow("myColName0", "myColType0").
		AddRow("myColName1", "myColType1")

	mockWrapper.mock.ExpectQuery(
		"SELECT a.column_name, a.column_type FROM information_schema.columns a WHERE a.TABLE_NAME = \\?;",
	).WithArgs("myTable").WillReturnRows(rows)

	result, err := makeMapColNameToColTypeName(context.Background(), mockWrapper.tx, "myTable")
	require.NoError(t, err)
	require.Equal(t, result, map[string]string{"myColName0": "myColType0", "myColName1": "myColType1"})
}

func Test_makeArrColsNames(t *testing.T) {
	tableSchema := []abstract.ColSchema{
		{ColumnName: "a"},
		{ColumnName: "b"},
		{ColumnName: "c"},
	}
	colNames := makeArrColsNames(tableSchema)
	require.Equal(t, colNames, []string{"a", "b", "c"})
}

func Test_calcChunkSize(t *testing.T) {
	require.Equal(t, uint64(100000), calcChunkSize(0, uint64(10*MB), 100*1000))

	require.Equal(t, uint64(100000), calcChunkSize(999999, uint64(100*KB), 100*1000))
	require.Equal(t, uint64(100000), calcChunkSize(999999, uint64(1*MB), 100*1000))
	require.Equal(t, uint64(100000), calcChunkSize(999999, uint64(10*MB), 100*1000))
	require.Equal(t, uint64(100000), calcChunkSize(999999, uint64(100*MB), 100*1000))
	require.Equal(t, uint64(48828), calcChunkSize(999999, uint64(1*GB), 100*1000))
	require.Equal(t, uint64(4882), calcChunkSize(999999, uint64(10*GB), 100*1000))
	require.Equal(t, uint64(488), calcChunkSize(999999, uint64(100*GB), 100*1000))
	require.Equal(t, uint64(100), calcChunkSize(999999, uint64(1*TB), 100*1000))

	require.Equal(t, uint64(100000), calcChunkSize(499999, uint64(10*MB), 100*1000))
	require.Equal(t, uint64(100000), calcChunkSize(499999, uint64(100*MB), 100*1000))
	require.Equal(t, uint64(24414), calcChunkSize(499999, uint64(1*GB), 100*1000))
	require.Equal(t, uint64(2441), calcChunkSize(499999, uint64(10*GB), 100*1000))
}

func Test_getTableRowsCountTableDataSizeInBytes(t *testing.T) {
	mockWrapper, err := makeTxMock()
	require.NoError(t, err)
	defer mockWrapper.Close()

	rows0 := sqlmock.NewRows([]string{"TABLE_ROWS"}).
		AddRow(123)

	mockWrapper.mock.ExpectQuery(`
		SELECT
			COALESCE\(TABLE_ROWS, 0\)
		FROM information_schema.tables
		WHERE TABLE_NAME = \? AND table_schema = \?`,
	).WithArgs("myTableName", "myDatabaseName").WillReturnRows(rows0)

	rows1 := sqlmock.NewRows([]string{"data_length"}).
		AddRow(234)

	mockWrapper.mock.ExpectQuery(`
		SELECT
			COALESCE\(data_length, 0\)
		FROM information_schema.tables
		WHERE TABLE_NAME = \? AND table_schema = \?`,
	).WithArgs("myTableName", "myDatabaseName").WillReturnRows(rows1)

	table := abstract.TableDescription{Name: "myTableName", Schema: "myDatabaseName"}

	tableRowsCount, tableDataSizeInBytes, err := getTableRowsCountTableDataSizeInBytes(context.Background(), mockWrapper.tx, table)
	require.NoError(t, err)

	require.Equal(t, tableRowsCount, uint64(123))
	require.Equal(t, tableDataSizeInBytes, uint64(234))
}

//---------------------------------------------------------------------------------------------------------------------

func Test_buildSelectQuery(t *testing.T) {
	table00 := abstract.TableDescription{Schema: "db", Name: "tableName", Filter: "", Offset: 0}
	tableSchema00 := []abstract.ColSchema{
		{ColumnName: "col1"},
		{ColumnName: "col2"},
		{ColumnName: "col2"},
	}
	query00 := buildSelectQuery(table00, tableSchema00)
	require.Equal(t, query00, "SELECT `col1`, `col2`, `col2` FROM `db`.`tableName` ")

	//---

	table01 := abstract.TableDescription{Schema: "db", Name: "tableName", Filter: "1=1", Offset: 1}
	tableSchema01 := []abstract.ColSchema{
		{ColumnName: "col1"},
		{ColumnName: "col2"},
		{ColumnName: "col2"},
	}
	query01 := buildSelectQuery(table01, tableSchema01)
	require.Equal(t, query01, "SELECT `col1`, `col2`, `col2` FROM `db`.`tableName`  WHERE 1=1 OFFSET 1")
}

func Test_orderByPrimaryKeys(t *testing.T) {
	tableSchema := []abstract.ColSchema{
		{ColumnName: "colName1", PrimaryKey: true},
		{ColumnName: "colName2", PrimaryKey: true},
		{ColumnName: "colName3", PrimaryKey: false},
	}
	result0, _ := OrderByPrimaryKeys(tableSchema, "ASC")
	require.Equal(t, result0, " ORDER BY `colName1` ASC,`colName2` ASC")
	result1, _ := OrderByPrimaryKeys(tableSchema, "DESC")
	require.Equal(t, result1, " ORDER BY `colName1` DESC,`colName2` DESC")
}

//--------------------------------------------------------------------------------------------------------------------

func Test_prepareArrayWithTypes_00(t *testing.T) {
	var myInt int
	var myString string
	var myUint64 uint64

	var sqlRowsMock SQLRowsMock
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName1", reflect.TypeOf(myInt)))
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName2", reflect.TypeOf(myString)))
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName3", reflect.TypeOf(myUint64)))

	colNameToColTypeName := map[string]string{
		"colName1": "int(10) unsigned",
		"colName2": "text",
		"colName3": "bigint(20) unsigned",
	}

	arr, err := prepareArrayWithTypes(&sqlRowsMock, colNameToColTypeName, time.Local)
	require.NoError(t, err)

	require.Equal(t, len(arr), 3)
	require.Equal(t, "*int", fmt.Sprintf("%T", arr[0]))
	require.Equal(t, "*string", fmt.Sprintf("%T", arr[1]))
	require.Equal(t, "*uint64", fmt.Sprintf("%T", arr[2]))
}

func Test_prepareArrayWithTypes_01(t *testing.T) {
	var myInt int
	var myString string
	var myUint64 uint64

	var sqlRowsMock SQLRowsMock
	errorForSQLRows := "test"
	sqlRowsMock.err = &errorForSQLRows
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName1", reflect.TypeOf(myInt)))
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName2", reflect.TypeOf(myString)))
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName3", reflect.TypeOf(myUint64)))

	colNameToColTypeName := map[string]string{
		"colName1": "int(10) unsigned",
		"colName2": "text",
		"colName3": "bigint(20) unsigned",
	}

	arr, err := prepareArrayWithTypes(&sqlRowsMock, colNameToColTypeName, time.Local)
	require.Error(t, err, "test")
	require.Equal(t, len(arr), 0)
}

func Test_pushDropAndCreateDDL(t *testing.T) {
	mockWrapper, err := makeTxMock()
	require.NoError(t, err)
	defer mockWrapper.Close()

	rows := sqlmock.NewRows([]string{"table_name", "ddl"}).
		AddRow("myTableName", "blablabla CREATE TABLE")

	mockWrapper.mock.ExpectQuery(
		"SHOW CREATE TABLE `db`.`myTable`;",
	).WillReturnRows(rows)

	var finalChangeItems []abstract.ChangeItem
	commitTime := time.Now()

	err = pushCreateTable(
		context.Background(),
		mockWrapper.tx,
		abstract.TableID{Namespace: "db", Name: "myTable"},
		commitTime,
		func(changeItems []abstract.ChangeItem) error {
			finalChangeItems = changeItems
			return nil
		},
	)
	require.NoError(t, err)

	expectedChangeItems := []abstract.ChangeItem{
		{
			CommitTime:   uint64(commitTime.UnixNano()),
			Schema:       "db",
			Table:        "myTableName",
			Kind:         abstract.DDLKind,
			ColumnValues: []interface{}{"blablabla CREATE TABLE IF NOT EXISTS"},
		},
	}
	require.Equal(t, finalChangeItems, expectedChangeItems)
}

func Test_readRowsAndPushByChunks_00(t *testing.T) {
	var myInt int
	var myString string
	var myUint64 uint64

	var sqlRowsMock SQLRowsMock
	sqlRowsMock.ExpectedCallsNext = 3
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName1", reflect.TypeOf(myInt)))
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName2", reflect.TypeOf(myString)))
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName3", reflect.TypeOf(myUint64)))

	st := time.Now()
	table := abstract.TableDescription{Name: "myTableName", Schema: "myDatabaseName"}
	currTableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "a", DataType: schema.TypeInt32.String()},
		{ColumnName: "b", DataType: schema.TypeString.String()},
		{ColumnName: "c", DataType: schema.TypeInt64.String()},
	})
	colNameToColTypeName := map[string]string{
		"colName1": "int(10) unsigned",
		"colName2": "text",
		"colName3": "bigint(20) unsigned",
	}

	pusherCalls := 0

	err := readRowsAndPushByChunks(
		time.Local,
		&sqlRowsMock,
		st,
		table,
		currTableSchema,
		colNameToColTypeName,
		1,
		0,
		false,
		func(input []abstract.ChangeItem) error {
			pusherCalls++
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, pusherCalls, 3)
}

func Test_readRowsAndPushByChunks_01(t *testing.T) {
	var myInt int
	var myString string
	var myUint64 uint64

	var sqlRowsMock SQLRowsMock
	sqlRowsMock.ExpectedCallsNext = 3
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName1", reflect.TypeOf(myInt)))
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName2", reflect.TypeOf(myString)))
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName3", reflect.TypeOf(myUint64)))

	st := time.Now()
	table := abstract.TableDescription{Name: "myTableName", Schema: "myDatabaseName"}
	currTableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "a", DataType: schema.TypeInt32.String()},
		{ColumnName: "b", DataType: schema.TypeString.String()},
		{ColumnName: "c", DataType: schema.TypeInt64.String()},
	})
	colNameToColTypeName := map[string]string{
		"colName1": "int(10) unsigned",
		"colName2": "text",
		"colName3": "bigint(20) unsigned",
	}

	pusherCalls := 0

	err := readRowsAndPushByChunks(
		time.Local,
		&sqlRowsMock,
		st,
		table,
		currTableSchema,
		colNameToColTypeName,
		2,
		0,
		false,
		func(input []abstract.ChangeItem) error {
			pusherCalls++
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, pusherCalls, 2)
}

func Test_readRowsAndPushByChunks_BySize(t *testing.T) {
	var myInt int
	var myString string
	var myUint64 uint64

	var sqlRowsMock SQLRowsMock
	sqlRowsMock.rowsFunc = func() []interface{} {
		return []interface{}{
			123,
			strings.Repeat("_long-string_", 1024*1024), // ~15 MiB of data
			uint64(123),
		}
	}
	sqlRowsMock.ExpectedCallsNext = 3
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName1", reflect.TypeOf(myInt)))
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName2", reflect.TypeOf(myString)))
	sqlRowsMock.ColTypes = append(sqlRowsMock.ColTypes, makeSQLColumnType("colName3", reflect.TypeOf(myUint64)))

	st := time.Now()
	table := abstract.TableDescription{Name: "myTableName", Schema: "myDatabaseName"}
	currTableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "a", DataType: schema.TypeInt32.String()},
		{ColumnName: "b", DataType: schema.TypeString.String()},
		{ColumnName: "c", DataType: schema.TypeInt64.String()},
	})
	colNameToColTypeName := map[string]string{
		"colName1": "int(10) unsigned",
		"colName2": "text",
		"colName3": "bigint(20) unsigned",
	}

	pusherCalls := 0

	err := readRowsAndPushByChunks(
		time.Local,
		&sqlRowsMock,
		st,
		table,
		currTableSchema,
		colNameToColTypeName,
		200,
		0,
		false,
		func(input []abstract.ChangeItem) error {
			pusherCalls++
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, pusherCalls, 2)
}
