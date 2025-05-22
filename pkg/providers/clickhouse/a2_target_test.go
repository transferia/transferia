package clickhouse

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema"
	"github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"github.com/transferia/transferia/pkg/stats"
	"go.uber.org/mock/gomock"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

func TestTargetPush(t *testing.T) {
	tID := abstract.TableID{Namespace: "mtmobproxy", Name: "foo_bar"}
	tInfo := abstract.TableInfo{
		Schema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "ServerName", DataType: string(yt_schema.TypeString), OriginalType: "ch:String"},
			{ColumnName: "DC", DataType: string(yt_schema.TypeString), OriginalType: "ch:FixedString(3)"},
			{ColumnName: "RequestDate", DataType: string(yt_schema.TypeDate), OriginalType: "ch:Date"},
			{ColumnName: "RequestDateTime", DataType: string(yt_schema.TypeDatetime), OriginalType: "ch:DateTime"},
			{ColumnName: "VirtualHost", DataType: string(yt_schema.TypeString), OriginalType: "ch:String"},
			{ColumnName: "Path", DataType: string(yt_schema.TypeString), OriginalType: "ch:String"},
			{ColumnName: "BasePath", DataType: string(yt_schema.TypeString), OriginalType: "ch:String"},
			{ColumnName: "Code", DataType: string(yt_schema.TypeUint16), OriginalType: "ch:UInt16"},
			{ColumnName: "RequestLengthBytes", DataType: string(yt_schema.TypeUint32), OriginalType: "ch:UInt32"},
			{ColumnName: "FullRequestTime", DataType: string(yt_schema.TypeUint16), OriginalType: "ch:UInt16"},
		}),
	}
	chDst, err := chrecipe.Target(chrecipe.WithInitFile("gotest/dump.sql"))
	require.NoError(t, err)
	sinkParams, err := chDst.ToSinkParams(&dp_model.Transfer{})
	require.NoError(t, err)
	client, err := httpclient.NewHTTPClientImpl(sinkParams)
	require.NoError(t, err)
	target, err := newHTTPTargetImpl(new(dp_model.Transfer), sinkParams, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	require.NoError(t, err)
	require.NoError(t, <-target.AsyncPush(
		&schema.DDLBatch{
			DDLs: []schema.TableDDL{
				schema.NewTableDDL(
					tID,
					"CREATE TABLE mtmobproxy.foo_bar ("+
						"`ServerName` String,"+
						" `DC` FixedString(3),"+
						" `RequestDate` Date,"+
						" `RequestDateTime` DateTime,"+
						" `VirtualHost` String,"+
						" `Path` String,"+
						" `BasePath` String DEFAULT 'misc',"+
						" `Code` UInt16,"+
						" `RequestLengthBytes` UInt32,"+
						" `FullRequestTime` UInt16"+
						") ENGINE = MergeTree PARTITION BY toMonday(RequestDate) "+
						"ORDER BY (BasePath, Code, ServerName) "+
						"SETTINGS index_granularity = 8192",
					"MergeTree",
				),
			},
		},
	))
	dataBytes := []byte(`["server_name","IVA","2021-12-16","2019-01-01 00:00:00","some-host","path",null,123,321,3213213]
["my-server2", "iva","2019-01-01","2019-01-01 00:00:00","some-host","path",null,123,321,3213213]`)
	require.NoError(
		t,
		<-target.AsyncPush(
			NewJSONCompactBatch(
				NewTablePart(1, 1, "_", tID, TablePart{}, ""),
				dataBytes,
				tInfo.Schema,
				time.Now(),
				2,
				len(dataBytes),
			),
		),
	)
	var res int
	require.NoError(t, client.Query(context.Background(), logger.Log, target.HostByPart(nil), `select count(distinct ServerName) from mtmobproxy.foo_bar;`, &res))
	require.Equal(t, res, 2)
}

var chDstModel = `
{
  "TTL": "",
  "User": "my_lovely_user",
  "Hosts": null,
  "Token": "AQAD-blablabla",
  "Shards": null,
  "AltNamesList": [
	{
    	"From": "statistics_rep",
		"To": "statistics_new_transfer"
	},
	{
		"From": "market_orders_rep",
		"To": "market_orders_rep_transfer"
	}
  ],
  "Cleanup": "Disabled",
  "Cluster": "179f2d18-5c22-4e2b-9558-55f605a410ca",
  "Database": "distribution_ch_test",
  "HTTPPort": 8443,
  "Interval": 1000000000,
  "Password": "blablabla",
  "Rotation": null,
  "ShardCol": "",
  "ForceHTTP": false,
  "Partition": "",
  "NativePort": 9440,
  "RetryCount": 20,
  "SSLEnabled": true,
  "AnyAsString": false,
  "InferSchema": false,
  "IsUpdateable": false,
  "StoreOffsets": false,
  "SubNetworkID": "",
  "ChClusterName": "",
  "ColumnToShard": {},
  "InflightBuffer": 50000,
  "PemFileContent": "",
  "MigrationOptions": {
    "AddNewColumns": false
  },
  "SecurityGroupIDs": null,
  "ColumnToShardName": {},
  "ShardByTransferID": false,
  "TransformerConfig": null,
  "SystemColumnsFirst": false,
  "ProtocolUnspecified": false,
  "UseSchemaInTableName": false
}
`

func createTargetWithTopology(t *testing.T, cluster *topology.Cluster) *HTTPTarget {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var chDst model.ChDestination
	err := json.Unmarshal([]byte(chDstModel), &chDst)
	require.NoError(t, err)

	client := httpclient.NewMockHTTPClient(ctrl)

	cfg := chDst.ToReplicationFromPGSinkParams()
	mtrc := solomon.NewRegistry(solomon.NewRegistryOpts())
	target := &HTTPTarget{
		client:         client,
		config:         cfg,
		logger:         logger.Log,
		cluster:        cluster,
		altNames:       MakeAltNames(cfg),
		metrics:        stats.NewSinkerStats(mtrc),
		wrapperMetrics: stats.NewWrapperStats(mtrc),
	}

	return target
}

func TestAdjustDDLToTarget(t *testing.T) {
	cluster := &topology.Cluster{
		Topology: *topology.NewTopology("179f2d18-5c22-4e2b-9558-55f605a410ca", false),
		Shards: topology.ShardHostMap{1: {
			&clickhouse.Host{
				Name: "sas-z7pefafuvw6ss0et.db.yandex.net",
			},
			&clickhouse.Host{
				Name: "man-a3fg8ewmwkxflr7h.db.yandex.net",
			},
			&clickhouse.Host{
				Name: "vla-a8srnet6jntawnm0.db.yandex.net",
			},
		}},
	}
	target := createTargetWithTopology(t, cluster)

	t.Run("case when distributed DDL", func(t *testing.T) {
		res, err := target.adjustDDLToTarget(schema.NewTableDDL(
			abstract.TableID{Namespace: "db", Name: "test_distributed_ddl"},
			"CREATE TABLE db.test_distr ON CLUSTER abc (dt Date, data String) "+
				"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db/test_distr', '{replica}') "+
				"PARTITION BY dt ORDER BY tuple()",
			"ReplicatedMergeTree"), true)
		require.NoError(t, err)
		require.Equal(t, "CREATE TABLE IF NOT EXISTS `distribution_ch_test`.test_distr  ON CLUSTER `179f2d18-5c22-4e2b-9558-55f605a410ca` "+
			"(dt Date, data String) "+
			"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db/test_distr', '{replica}') "+
			"PARTITION BY dt ORDER BY tuple()", res)
	})

	t.Run("case when non-distributed DDL", func(t *testing.T) {
		res, err := target.adjustDDLToTarget(schema.NewTableDDL(
			abstract.TableID{Namespace: "db", Name: "test_distributed_ddl"},
			"CREATE TABLE db.test_distr (dt Date, data String) "+
				"ENGINE = MergeTree() "+
				"PARTITION BY dt ORDER BY tuple()",
			"MergeTree"), true)
		require.NoError(t, err)
		require.Equal(t, "CREATE TABLE IF NOT EXISTS `distribution_ch_test`.test_distr  ON CLUSTER `179f2d18-5c22-4e2b-9558-55f605a410ca` "+
			"(dt Date, data String) "+
			"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db.test_distributed_ddl_cdc', '{replica}') "+
			"PARTITION BY dt ORDER BY tuple()", res)
	})

	t.Run("case when distributed DDL", func(t *testing.T) {
		res, err := target.adjustDDLToTarget(schema.NewTableDDL(
			abstract.TableID{Namespace: "db", Name: "test_distributed_ddl"},
			"CREATE TABLE db.test_distr ON CLUSTER abc (dt Date, data String) "+
				"ENGINE = SharedMergeTree('/clickhouse/tables/{shard}/db/test_distr', '{replica}') "+
				"PARTITION BY dt ORDER BY tuple()",
			"SharedMergeTree"), true)
		require.NoError(t, err)
		require.Equal(t, "CREATE TABLE IF NOT EXISTS `distribution_ch_test`.test_distr  ON CLUSTER `179f2d18-5c22-4e2b-9558-55f605a410ca` "+
			"(dt Date, data String) "+
			"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db/test_distr', '{replica}') "+
			"PARTITION BY dt ORDER BY tuple()", res)
	})
}
