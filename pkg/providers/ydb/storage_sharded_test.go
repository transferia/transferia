//go:build !disable_ydb_provider

package ydb

import (
	"context"
	"crypto/tls"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	tableName = "test_table_sharded"
)

func TestYdbStorageSharded_TableLoad(t *testing.T) {
	endpoint, ok := os.LookupEnv("YDB_ENDPOINT")
	if !ok {
		t.Fail()
	}
	prefix, ok := os.LookupEnv("YDB_DATABASE")
	if !ok {
		t.Fail()
	}
	token, ok := os.LookupEnv("YDB_TOKEN")
	if !ok {
		token = "anyNotEmptyString"
	}

	src := &YdbSource{
		Token:    model.SecretString(token),
		Database: prefix,
		Instance: endpoint,
		Tables:   []string{tableName},
		TableColumnsFilter: []YdbColumnsFilter{{
			TableNamesRegexp:  "^foo_t_.*",
			ColumnNamesRegexp: "raw_value",
			Type:              YdbColumnsBlackList,
		}},
		SubNetworkID:      "",
		Underlay:          false,
		ServiceAccountID:  "",
		IsSnapshotSharded: true,
		CopyFolder:        "test-folder",
	}

	st, err := NewStorage(src.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	var tlsConfig *tls.Config
	clientCtx := context.Background()

	ydbCreds, err := ResolveCredentials(
		src.UserdataAuth,
		string(src.Token),
		JWTAuthParams{
			KeyContent:      src.SAKeyContent,
			TokenServiceURL: src.TokenServiceURL,
		},
		src.ServiceAccountID,
		nil,
		logger.Log,
	)
	require.NoError(t, err)

	ydbDriver, err := newYDBDriver(clientCtx, src.Database, src.Instance, ydbCreds, tlsConfig, false)
	require.NoError(t, err)

	err = ydbDriver.Table().Do(clientCtx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(ydbDriver.Name(), tableName),
				options.WithColumn("c_custkey", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("c_custkey"),
				options.WithPartitions(options.WithUniformPartitions(4)),
			)
		},
	)
	require.NoError(t, err)

	err = st.BeginSnapshot(clientCtx)
	require.NoError(t, err)
	content, err := ydbDriver.Scheme().ListDirectory(clientCtx, path.Join(src.Database, src.CopyFolder))
	require.NoError(t, err)
	require.Equal(t, 1, len(content.Children))
	require.Equal(t, tableName, content.Children[0].Name)

	result, err := st.ShardTable(clientCtx, abstract.TableDescription{Name: tableName, Schema: ""})
	require.NoError(t, err)
	require.Equal(t, 4, len(result))
	for i, part := range result {
		require.Equal(t, uint64(i), part.Offset)
	}

	err = st.EndSnapshot(clientCtx)
	require.NoError(t, err)
	_, err = ydbDriver.Scheme().ListDirectory(clientCtx, path.Join(src.Database, src.CopyFolder))
	require.Error(t, err)
}
