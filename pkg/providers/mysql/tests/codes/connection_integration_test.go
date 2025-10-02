package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
)

func TestConnect_InvalidCredential_ReturnsCodedError(t *testing.T) {

	src := mysqlrecipe.RecipeMysqlSource()
	connParams, err := mysql.NewConnectionParams(src.ToStorageParams())
	require.NoError(t, err)

	// Make credentials invalid
	connParams.Password = "this-password-is-not-correct"

	db, err := mysql.Connect(connParams, nil)
	if db != nil {
		_ = db.Close()
	}
	require.Error(t, err)
	if !codes.InvalidCredential.Contains(err) {
		t.Fatalf("expected codes.InvalidCredential, got: %v", err)
	}
}

func TestConnect_DialError_ReturnsCodedError(t *testing.T) {
	src := mysqlrecipe.RecipeMysqlSource()
	connParams, err := mysql.NewConnectionParams(src.ToStorageParams())
	require.NoError(t, err)

	// Укажем несуществующий порт, чтобы сорвать dial
	connParams.Port = 65000 // почти наверняка свободен локально

	db, err := mysql.Connect(connParams, nil)
	if db != nil {
		_ = db.Close()
	}
	require.Error(t, err)
	if !codes.Dial.Contains(err) {
		t.Fatalf("expected codes.Dial, got: %v", err)
	}
}

func TestConnect_DNSResolutionFailed_ReturnsCodedError(t *testing.T) {
	params := &mysql.ConnectionParams{
		Host:     "nonexistent.invalid", // гарантированно не существует
		Port:     3306,
		User:     "user",
		Password: "password",
		Database: "db",
	}
	db, err := mysql.Connect(params, nil)
	if db != nil {
		_ = db.Close()
	}
	require.Error(t, err)
	if !codes.MySQLDNSResolutionFailed.Contains(err) {
		t.Fatalf("expected codes.MySQLDNSResolutionFailed, got: %v", err)
	}
}

func TestConnect_UnknownDatabase_ReturnsCodedError(t *testing.T) {

	src := mysqlrecipe.RecipeMysqlSource()
	connParams, err := mysql.NewConnectionParams(src.ToStorageParams())
	require.NoError(t, err)
	connParams.Database = "definitely_does_not_exist_db"

	db, err := mysql.Connect(connParams, nil)
	if db != nil {
		_ = db.Close()
	}
	require.Error(t, err)
	if !codes.MySQLUnknownDatabase.Contains(err) {
		t.Fatalf("expected codes.MySQLUnknownDatabase, got: %v", err)
	}
}
