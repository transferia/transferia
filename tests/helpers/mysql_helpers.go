package helpers

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"go.ytsaurus.tech/library/go/core/log"
)

var RecipeMysqlSource = mysqlrecipe.RecipeMysqlSource
var RecipeMysqlTarget = mysqlrecipe.RecipeMysqlTarget
var RecipeMysqlSourceWithConnection = mysqlrecipe.RecipeMysqlSourceWithConnection
var RecipeMysqlTargetWithConnection = mysqlrecipe.RecipeMysqlTargetWithConnection
var WithMysqlInclude = mysqlrecipe.WithMysqlInclude

func ExecuteMySQLStatement(t *testing.T, statement string, connectionParams *mysql.ConnectionParams) {
	require.NoError(t, mysqlrecipe.Exec(statement, connectionParams))
}

func ExecuteMySQLStatementsLineByLine(t *testing.T, statements string, connectionParams *mysql.ConnectionParams) {
	conn, err := mysql.Connect(connectionParams, nil)
	require.NoError(t, err)
	defer conn.Close()

	for _, line := range strings.Split(statements, "\n") {
		_, err = conn.Exec(line)
		if err != nil && !isEmptyQueryError(err) {
			require.Fail(t, err.Error())
		}
	}
}

func isEmptyQueryError(err error) bool {
	driverError, ok := err.(*mysqldriver.MySQLError)
	if !ok {
		return false
	}
	const emptyQueryErrorNumber = 1065
	return driverError.Number == emptyQueryErrorNumber
}

func MySQLDump(t *testing.T, storageParams *mysql.MysqlStorageParams) string {
	mysqlDumpPath := os.Getenv("RECIPE_MYSQLDUMP_BINARY")
	args := []string{
		"--host", storageParams.Host,
		"--user", storageParams.User,
		"--port", fmt.Sprintf("%d", storageParams.Port),
		"--databases", storageParams.Database,
		"--force",
		"--skip-extended-insert",
		"--dump-date=false",
		"--default-character-set=utf8mb4",
		fmt.Sprintf("--ignore-table=%s.__tm_gtid_keeper", storageParams.Database),
		fmt.Sprintf("--ignore-table=%s.__tm_keeper", storageParams.Database),
		fmt.Sprintf("--ignore-table=%s.__table_transfer_progress", storageParams.Database),
	}
	command := exec.Command(mysqlDumpPath, args...)
	command.Env = append(command.Env, fmt.Sprintf("MYSQL_PWD=%s", storageParams.Password))
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	logger.Log.Info("Run mysqldump", log.String("path", mysqlDumpPath), log.Array("args", args))
	require.NoError(t, command.Run(), stderr.String())
	logger.Log.Warnf("stderr\n%s", stderr.String())
	return stdout.String()
}

func NewMySQLConnectionParams(t *testing.T, storageParams *mysql.MysqlStorageParams) *mysql.ConnectionParams {
	connParams, err := mysql.NewConnectionParams(storageParams)
	require.NoError(t, err)
	return connParams
}

func NewMySQLStorageFromSource(t *testing.T, src *mysql.MysqlSource) *mysql.Storage {
	storage, err := mysql.NewStorage(src.ToStorageParams())
	require.NoError(t, err)
	return storage
}

func NewMySQLStorageFromTarget(t *testing.T, dst *mysql.MysqlDestination) *mysql.Storage {
	storage, err := mysql.NewStorage(dst.ToStorageParams())
	require.NoError(t, err)
	return storage
}
