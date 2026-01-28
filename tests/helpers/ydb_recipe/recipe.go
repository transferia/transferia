package ydbrecipe

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func Driver(t *testing.T, opts ...ydb.Option) *ydb.Driver {
	instance, port, database, creds := InstancePortDatabaseCreds(t)
	dsn := sugar.DSN(fmt.Sprintf("%s:%d", instance, port), database)
	if creds != nil {
		opts = append(opts, ydb.WithCredentials(creds))
	}
	driver, err := ydb.Open(context.Background(), dsn, opts...)
	require.NoError(t, err)

	return driver
}

func InstancePortDatabaseCreds(t *testing.T) (string, int, string, credentials.Credentials) {
	parts := strings.Split(os.Getenv("YDB_ENDPOINT"), ":")
	require.Len(t, parts, 2)

	instance := parts[0]
	port, err := strconv.Atoi(parts[1])
	require.NoError(t, err)

	database := os.Getenv("YDB_DATABASE")
	if database == "" {
		database = "local"
	}

	var creds credentials.Credentials
	token := Token()
	if token != "" {
		creds = credentials.NewAccessTokenCredentials(token)
	}

	return instance, port, database, creds
}

func Token() string {
	return os.Getenv("YDB_TOKEN")
}
