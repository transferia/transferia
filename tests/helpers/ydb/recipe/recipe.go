package ydbrecipe

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_credentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func Driver(t *testing.T, opts ...ydb_go_sdk.Option) *ydb_go_sdk.Driver {
	instance, port, database, creds := InstancePortDatabaseCreds(t)
	dsn := sugar.DSN(fmt.Sprintf("%s:%d", instance, port), database)
	if creds != nil {
		opts = append(opts, ydb_go_sdk.WithCredentials(creds))
	}
	driver, err := ydb_go_sdk.Open(context.Background(), dsn, opts...)
	require.NoError(t, err)

	return driver
}

func InstancePortDatabaseCreds(t *testing.T) (string, int, string, ydb_credentials.Credentials) {
	parts := strings.Split(os.Getenv("YDB_ENDPOINT"), ":")
	require.Len(t, parts, 2)

	instance := parts[0]
	port, err := strconv.Atoi(parts[1])
	require.NoError(t, err)

	database := os.Getenv("YDB_DATABASE")
	if database == "" {
		database = "local"
	}

	var creds ydb_credentials.Credentials
	token := Token()
	if token != "" {
		creds = ydb_credentials.NewAccessTokenCredentials(token)
	}

	return instance, port, database, creds
}

func Token() string {
	return os.Getenv("YDB_TOKEN")
}
