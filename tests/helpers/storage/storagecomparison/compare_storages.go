package storagecomparison

import (
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	provider_clickhouse "github.com/transferia/transferia/pkg/providers/clickhouse"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_mongo "github.com/transferia/transferia/pkg/providers/mongo"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_storage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	"go.ytsaurus.tech/library/go/core/log"
)

var technicalTables = map[string]bool{
	"__data_transfer_signal_table": true, // dblog signal table
	"__consumer_keeper":            true, // pg
	"__dt_cluster_time":            true, // mongodb
	"__table_transfer_progress":    true, // mysql
	"__tm_gtid_keeper":             true, // mysql
	"__tm_keeper":                  true, // mysql
}

func withTextSerialization(storageParams *provider_postgres.PgStorageParams) *provider_postgres.PgStorageParams {
	// Checksum does not support comparing binary values for now. Use
	// the text types instead, even in homogeneous pg->pg transfers.
	storageParams.UseBinarySerialization = false
	return storageParams
}

func GetSampleableStorageByModel(t *testing.T, serverModel interface{}) abstract.ChecksumableStorage {
	var result abstract.ChecksumableStorage
	var err error

	switch model := serverModel.(type) {
	// pg
	case provider_postgres.PgSource:
		result, err = provider_postgres.NewStorage(withTextSerialization(model.ToStorageParams(nil)))
	case *provider_postgres.PgSource:
		result, err = provider_postgres.NewStorage(withTextSerialization(model.ToStorageParams(nil)))
	case provider_postgres.PgDestination:
		result, err = provider_postgres.NewStorage(withTextSerialization(model.ToStorageParams()))
	case *provider_postgres.PgDestination:
		result, err = provider_postgres.NewStorage(withTextSerialization(model.ToStorageParams()))
	// ch
	case clickhouse_model.ChSource:
		storageParams, storageParamsErr := model.ToStorageParams()
		require.NoError(t, storageParamsErr)
		result, err = provider_clickhouse.NewStorage(storageParams, nil)
	case *clickhouse_model.ChSource:
		storageParams, storageParamsErr := model.ToStorageParams()
		require.NoError(t, storageParamsErr)
		result, err = provider_clickhouse.NewStorage(storageParams, nil)
	case clickhouse_model.ChDestination:
		storageParams, storageParamsErr := model.ToStorageParams()
		require.NoError(t, storageParamsErr)
		result, err = provider_clickhouse.NewStorage(storageParams, nil)
	case *clickhouse_model.ChDestination:
		storageParams, storageParamsErr := model.ToStorageParams()
		require.NoError(t, storageParamsErr)
		result, err = provider_clickhouse.NewStorage(storageParams, nil)
	// mysql
	case provider_mysql.MysqlSource:
		result, err = provider_mysql.NewStorage(model.ToStorageParams())
	case *provider_mysql.MysqlSource:
		result, err = provider_mysql.NewStorage(model.ToStorageParams())
	case provider_mysql.MysqlDestination:
		result, err = provider_mysql.NewStorage(model.ToStorageParams())
	case *provider_mysql.MysqlDestination:
		result, err = provider_mysql.NewStorage(model.ToStorageParams())
	// mongo
	case provider_mongo.MongoSource:
		result, err = provider_mongo.NewStorage(model.ToStorageParams())
	case *provider_mongo.MongoSource:
		result, err = provider_mongo.NewStorage(model.ToStorageParams())
	case provider_mongo.MongoDestination:
		result, err = provider_mongo.NewStorage(model.ToStorageParams())
	case *provider_mongo.MongoDestination:
		result, err = provider_mongo.NewStorage(model.ToStorageParams())
	// yt
	case provider_yt.YtDestination:
		result, err = yt_storage.NewStorage(model.ToStorageParams())
	case *provider_yt.YtDestination:
		result, err = yt_storage.NewStorage(model.ToStorageParams())
	case provider_yt.YtDestinationWrapper:
		result, err = yt_storage.NewStorage(model.ToStorageParams())
	case *provider_yt.YtDestinationWrapper:
		result, err = yt_storage.NewStorage(model.ToStorageParams())
	// ydb for now only works for small tables
	case provider_ydb.YdbDestination:
		result, err = provider_ydb.NewStorage(model.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
	case *provider_ydb.YdbDestination:
		result, err = provider_ydb.NewStorage(model.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
	case provider_ydb.YdbSource:
		result, err = provider_ydb.NewStorage(model.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
	case *provider_ydb.YdbSource:
		result, err = provider_ydb.NewStorage(model.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
	default:
		require.Fail(t, fmt.Sprintf("unknown type of serverModel: %T", serverModel))
	}

	if err != nil {
		require.Fail(t, fmt.Sprintf("unable to create storage: %s", err))
	}

	return result
}

func FilterTechnicalTables(tables abstract.TableMap) []abstract.TableDescription {
	result := make([]abstract.TableDescription, 0)
	for _, el := range tables.ConvertToTableDescriptions() {
		if technicalTables[el.Name] {
			continue
		}
		result = append(result, el)
	}
	return result
}

type CompareStoragesParams struct {
	EqualDataTypes      func(lDataType, rDataType string) bool
	TableFilter         func(tables abstract.TableMap) []abstract.TableDescription
	PriorityComparators []tasks.ChecksumComparator
}

func NewCompareStorageParams() *CompareStoragesParams {
	return &CompareStoragesParams{
		EqualDataTypes:      strictEquality,
		TableFilter:         FilterTechnicalTables,
		PriorityComparators: nil,
	}
}

func (p *CompareStoragesParams) WithEqualDataTypes(equalDataTypes func(lDataType, rDataType string) bool) *CompareStoragesParams {
	p.EqualDataTypes = equalDataTypes
	return p
}

func (p *CompareStoragesParams) WithTableFilter(tableFilter func(tables abstract.TableMap) []abstract.TableDescription) *CompareStoragesParams {
	p.TableFilter = tableFilter
	return p
}

func (p *CompareStoragesParams) WithPriorityComparators(comparators ...tasks.ChecksumComparator) *CompareStoragesParams {
	p.PriorityComparators = comparators
	return p
}

func CompareStorages(t *testing.T, sourceModel, targetModel interface{}, params *CompareStoragesParams) error {
	srcStorage := GetSampleableStorageByModel(t, sourceModel)
	dstStorage := GetSampleableStorageByModel(t, targetModel)
	switch src := srcStorage.(type) {
	case *provider_mysql.Storage:
		dst, ok := dstStorage.(*provider_mysql.Storage)
		if ok {
			src.IsHomo = true
			dst.IsHomo = true
		}
	}
	all, err := srcStorage.TableList(nil)
	require.NoError(t, err)
	return tasks.CompareChecksum(
		srcStorage,
		dstStorage,
		params.TableFilter(all),
		logger.Log,
		testmetrics.EmptyRegistry(),
		params.EqualDataTypes,
		&tasks.ChecksumParameters{
			TableSizeThreshold:  0,
			Tables:              nil,
			PriorityComparators: params.PriorityComparators,
		},
	)
}

func WaitStoragesSynced(t *testing.T, sourceModel, targetModel interface{}, retries uint64, compareParams *CompareStoragesParams) error {
	err := backoff.Retry(func() error {
		err := CompareStorages(t, sourceModel, targetModel, compareParams)
		if err != nil {
			logger.Log.Info("storage comparison failed", log.Error(err))
		}
		return err
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(2*time.Second), retries))
	return err
}

func CheckRowsCount(t *testing.T, serverModel interface{}, schema, tableName string, expectedRows uint64) {
	storage := GetSampleableStorageByModel(t, serverModel)
	tableDescr := abstract.TableDescription{Name: tableName, Schema: schema}
	rowsInSrc, err := storage.ExactTableRowsCount(tableDescr.ID())
	require.NoError(t, err)
	require.Equal(t, int(expectedRows), int(rowsInSrc))
}

func CheckRowsGreaterOrEqual(t *testing.T, serverModel interface{}, schema, tableName string, expectedMinumumRows uint64) {
	storage := GetSampleableStorageByModel(t, serverModel)
	tableDescr := abstract.TableDescription{Name: tableName, Schema: schema}
	rowsInSrc, err := storage.ExactTableRowsCount(tableDescr.ID())
	require.NoError(t, err)
	require.GreaterOrEqual(t, int(rowsInSrc), int(expectedMinumumRows))
}

// strictEquality - default callback for checksum - just compare typeNames
func strictEquality(l, r string) bool {
	return l == r
}
