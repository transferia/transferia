package helpers

import (
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	chModel "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	mongoStorage "github.com/transferia/transferia/pkg/providers/mongo"
	mysqlStorage "github.com/transferia/transferia/pkg/providers/mysql"
	pgStorage "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/pkg/providers/yt"
	ytStorage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"github.com/transferia/transferia/pkg/worker/tasks"
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

func withTextSerialization(storageParams *pgStorage.PgStorageParams) *pgStorage.PgStorageParams {
	// Checksum does not support comparing binary values for now. Use
	// the text types instead, even in homogeneous pg->pg transfers.
	storageParams.UseBinarySerialization = false
	return storageParams
}

func GetSampleableStorageByModel(t *testing.T, serverModel interface{}) abstract.SampleableStorage {
	var result abstract.SampleableStorage
	var err error

	switch model := serverModel.(type) {
	// pg
	case pgStorage.PgSource:
		result, err = pgStorage.NewStorage(withTextSerialization(model.ToStorageParams(nil)))
	case *pgStorage.PgSource:
		result, err = pgStorage.NewStorage(withTextSerialization(model.ToStorageParams(nil)))
	case pgStorage.PgDestination:
		result, err = pgStorage.NewStorage(withTextSerialization(model.ToStorageParams()))
	case *pgStorage.PgDestination:
		result, err = pgStorage.NewStorage(withTextSerialization(model.ToStorageParams()))
	// ch
	case chModel.ChSource:
		storageParams, storageParamsErr := model.ToStorageParams()
		require.NoError(t, storageParamsErr)
		result, err = clickhouse.NewStorage(storageParams, nil)
	case *chModel.ChSource:
		storageParams, storageParamsErr := model.ToStorageParams()
		require.NoError(t, storageParamsErr)
		result, err = clickhouse.NewStorage(storageParams, nil)
	case chModel.ChDestination:
		storageParams, storageParamsErr := model.ToStorageParams()
		require.NoError(t, storageParamsErr)
		result, err = clickhouse.NewStorage(storageParams, nil)
	case *chModel.ChDestination:
		storageParams, storageParamsErr := model.ToStorageParams()
		require.NoError(t, storageParamsErr)
		result, err = clickhouse.NewStorage(storageParams, nil)
	// mysql
	case mysqlStorage.MysqlSource:
		result, err = mysqlStorage.NewStorage(model.ToStorageParams())
	case *mysqlStorage.MysqlSource:
		result, err = mysqlStorage.NewStorage(model.ToStorageParams())
	case mysqlStorage.MysqlDestination:
		result, err = mysqlStorage.NewStorage(model.ToStorageParams())
	case *mysqlStorage.MysqlDestination:
		result, err = mysqlStorage.NewStorage(model.ToStorageParams())
	// mongo
	case mongoStorage.MongoSource:
		result, err = mongoStorage.NewStorage(model.ToStorageParams())
	case *mongoStorage.MongoSource:
		result, err = mongoStorage.NewStorage(model.ToStorageParams())
	case mongoStorage.MongoDestination:
		result, err = mongoStorage.NewStorage(model.ToStorageParams())
	case *mongoStorage.MongoDestination:
		result, err = mongoStorage.NewStorage(model.ToStorageParams())
	// yt
	case yt.YtDestination:
		result, err = ytStorage.NewStorage(model.ToStorageParams())
	case *yt.YtDestination:
		result, err = ytStorage.NewStorage(model.ToStorageParams())
	case yt.YtDestinationWrapper:
		result, err = ytStorage.NewStorage(model.ToStorageParams())
	case *yt.YtDestinationWrapper:
		result, err = ytStorage.NewStorage(model.ToStorageParams())
	// ydb for now only works for small tables
	case ydb.YdbDestination:
		result, err = ydb.NewStorage(model.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
	case *ydb.YdbDestination:
		result, err = ydb.NewStorage(model.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
	case ydb.YdbSource:
		result, err = ydb.NewStorage(model.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
	case *ydb.YdbSource:
		result, err = ydb.NewStorage(model.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
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
		EqualDataTypes:      StrictEquality,
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
	case *mysqlStorage.Storage:
		dst, ok := dstStorage.(*mysqlStorage.Storage)
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
		EmptyRegistry(),
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
