package snapshot

import (
	"context"
	"crypto/tls"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	ydb_provider "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/pkg/providers/ydb/logadapter"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/pkg/xtls"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	ydbcreds "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = yt_provider.YtSource{
		Cluster: os.Getenv("YT_PROXY"),
		Proxy:   os.Getenv("YT_PROXY"),
		Paths:   []string{"//home/cdc/junk/test_table"},
		YtToken: "",
	}
	Target = ydb_provider.YdbDestination{
		Database:     os.Getenv("YDB_DATABASE"),
		Token:        model.SecretString(os.Getenv("YDB_TOKEN")),
		Instance:     os.Getenv("YDB_ENDPOINT"),
		Cleanup:      model.DisabledCleanup,
		LegacyWriter: true,
	}
)

func NewYdbDriverFromStorage(t *testing.T, cfg *ydb_provider.YdbStorageParams) *ydb.Driver {
	var err error
	var tlsConfig *tls.Config
	if cfg.TLSEnabled {
		tlsConfig, err = xtls.FromPath(cfg.RootCAFiles)
		require.NoError(t, err)
	}
	clientCtx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	var ydbCreds ydbcreds.Credentials
	ydbCreds, err = ydb_provider.ResolveCredentials(
		cfg.UserdataAuth,
		string(cfg.Token),
		ydb_provider.JWTAuthParams{
			KeyContent:      cfg.SAKeyContent,
			TokenServiceURL: cfg.TokenServiceURL,
		},
		cfg.ServiceAccountID,
		cfg.OAuth2Config,
		logger.Log,
	)
	require.NoError(t, err)

	ydbDriver, err := newYDBDriver(clientCtx, cfg.Database, cfg.Instance, ydbCreds, tlsConfig, false)
	require.NoError(t, err)

	return ydbDriver
}

func newYDBDriver(
	ctx context.Context,
	database, instance string,
	credentials ydbcreds.Credentials,
	tlsConfig *tls.Config,
	verboseTraces bool,
) (*ydb.Driver, error) {
	secure := tlsConfig != nil

	traceLevel := trace.DriverEvents
	if verboseTraces {
		traceLevel = trace.DetailsAll
	}
	// TODO: it would be nice to handle some common errors such as unauthenticated one
	// but YDB driver error design makes this task extremely painful
	return ydb.Open(
		ctx,
		sugar.DSN(instance, database, sugar.WithSecure(secure)),
		ydb.WithCredentials(credentials),
		ydb.WithTLSConfig(tlsConfig),
		logadapter.WithTraces(logger.Log, traceLevel),
	)
}

func init() {
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

var TestData = []map[string]interface{}{
	{
		"id":    0,
		"value": "Test utf8 string 1",
		"count": 4,
	},
	{
		"id":    1,
		"value": "Max Tyurin",
		"count": 1,
	},
	{
		"id":    2,
		"value": nil,
		"count": 0,
	},
}

var YtColumns = []schema.Column{
	{Name: "id", ComplexType: schema.TypeInt32},
	{Name: "value", ComplexType: schema.Optional{Item: schema.TypeString}},
	{Name: "count", ComplexType: schema.TypeInt32},
}

func prepareTargetTable(t *testing.T) {
	ctx := context.Background()

	driver := NewYdbDriverFromStorage(t, Target.ToStorageParams())
	defer driver.Close(ctx)

	err := driver.Query().Exec(ctx, `CREATE TABLE test_table (
		id Int32 NOT NULL,
		value Utf8,
		count Int32 NOT NULL,
	    PRIMARY KEY (id))`)
	require.NoError(t, err)
}

func createTestData(t *testing.T) {
	ytc, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, &yt.Config{Proxy: Source.Proxy})
	require.NoError(t, err)

	sch := schema.Schema{
		Strict:     nil,
		UniqueKeys: false,
		Columns:    YtColumns,
	}

	ctx := context.Background()
	wr, err := yt.WriteTable(ctx, ytc, ypath.NewRich(Source.Paths[0]).YPath(), yt.WithCreateOptions(yt.WithSchema(sch), yt.WithRecursive()))
	require.NoError(t, err)
	for _, row := range TestData {
		require.NoError(t, wr.Write(row))
	}
	require.NoError(t, wr.Commit())
}

func checkDataRow(t *testing.T, targetRow map[string]interface{}, testRow map[string]interface{}) {
	for k, v := range testRow {
		targetVal := targetRow[k]
		require.EqualValues(t, v, targetVal, "non-matching values for column %s (target type %T)", k, targetVal)
	}
}

func TestSnapshot(t *testing.T) {
	createTestData(t)

	prepareTargetTable(t)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	require.NoError(t, snapshotLoader.UploadV2(context.Background(), nil, nil))

	targetStorage := helpers.GetSampleableStorageByModel(t, Target)
	totalInserts := 0
	require.NoError(t, targetStorage.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "test_table",
		Schema: "",
	}, func(input []abstract.ChangeItem) error {
		for _, ci := range input {
			if ci.Kind == abstract.DropTableKind {
				require.Fail(t, "no drops are allowed during this test")
			}
			if ci.Kind != abstract.InsertKind {
				continue
			}
			targetRow := ci.AsMap()
			keyRaw, ok := targetRow["id"]
			if !ok {
				require.Fail(t, "faulty test: missing key column")
			}
			key, ok := keyRaw.(int32)
			if !ok {
				require.Fail(t, "key column is of wrong type", "wrong type %T", keyRaw)
			}
			checkDataRow(t, targetRow, TestData[key])
			totalInserts += 1
		}
		return nil
	}))

	require.Equal(t, len(TestData), totalInserts)
}
