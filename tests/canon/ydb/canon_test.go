package ydb

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/canon/validator"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestCanonSource(t *testing.T) {
	Source := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{"canon_table"},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		ChangeFeedMode:     ydb.ChangeFeedModeNewImage,
		UseFullPaths:       false,
	}
	Source.WithDefaults()
	runCanon(
		t,
		Source,
		"canon_table",
		validator.InitDone(t),
		validator.ValuesTypeChecker,
		validator.Canonizator(t),
		validator.TypesystemChecker(ydb.ProviderType, func(colSchema abstract.ColSchema) string {
			return strings.TrimPrefix(colSchema.OriginalType, "ydb:")
		}),
	)
}

func TestCanonLongPathSource(t *testing.T) {
	Source := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		ChangeFeedMode:     ydb.ChangeFeedModeNewImage,
		UseFullPaths:       false,
	}
	Source.WithDefaults()
	t.Run("enable_full_path", func(t *testing.T) {
		Source.Tables = []string{"foo/enable_full_path"}
		Source.UseFullPaths = true
		runCanon(t, Source, "foo/enable_full_path", validator.InitDone(t))
	})
	t.Run("disable_full_path", func(t *testing.T) {
		Source.Tables = []string{"foo/disable_full_path"}
		Source.UseFullPaths = false
		runCanon(t, Source, "foo/disable_full_path", validator.InitDone(t))
	})
}

func runCanon(t *testing.T, Source *ydb.YdbSource, tablePath string, validators ...func() abstract.Sinker) {
	Target := &ydb.YdbDestination{
		Database: Source.Database,
		Token:    Source.Token,
		Instance: Source.Instance,
	}
	Target.WithDefaults()
	sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	currChangeItem := helpers.YDBInitChangeItem(tablePath)
	require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))
	// null case
	nullChangeItem := helpers.YDBInitChangeItem(tablePath)
	require.Greater(t, len(nullChangeItem.ColumnNames), 0)
	require.Equal(t, "id", nullChangeItem.ColumnNames[0])
	nullChangeItem.ColumnValues[0] = 801640048
	for i := 1; i < len(nullChangeItem.ColumnValues); i++ {
		if nullChangeItem.TableSchema.Columns()[i].DataType == string(schema.TypeDate) ||
			nullChangeItem.TableSchema.Columns()[i].DataType == string(schema.TypeDatetime) ||
			nullChangeItem.TableSchema.Columns()[i].DataType == string(schema.TypeTimestamp) {
			continue
		}
		nullChangeItem.ColumnValues[i] = nil
	}
	require.NoError(t, sinker.Push([]abstract.ChangeItem{*nullChangeItem}))

	counter, waiterSink := validator.NewCounter()

	validators = append(validators, waiterSink)
	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		Source,
		&model.MockDestination{
			SinkerFactory: validator.New(model.IsStrictSource(Source), validators...),
			Cleanup:       model.DisabledCleanup,
		},
		abstract.TransferTypeSnapshotAndIncrement,
	)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	replicationChangeItem := helpers.YDBStmtInsert(t, tablePath, 2)
	require.NoError(t, sinker.Push([]abstract.ChangeItem{*replicationChangeItem}))

	require.NoError(t, helpers.WaitCond(time.Second*60,
		func() bool {
			if counter.GetSum() != 2 {
				logger.Log.Warnf(" counter rows sum (%v) is not equal to %v", counter.GetSum(), 2)
				return false
			}
			return true
		}))
}
