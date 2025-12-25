package mysql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors/codes"
	pmysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
)

type noopAsyncSink struct{}

func (n *noopAsyncSink) Close() error { return nil }
func (n *noopAsyncSink) AsyncPush(items []abstract.ChangeItem) chan error {
	ch := make(chan error, 1)
	ch <- nil
	return ch
}

func TestBinlogFirstFileMissing_ReturnsCodedError(t *testing.T) {
	src := mysqlrecipe.RecipeMysqlSource()
	connParams, err := pmysql.NewConnectionParams(src.ToStorageParams())
	require.NoError(t, err)

	db, err := pmysql.Connect(connParams, nil)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err = db.ExecContext(ctx, "FLUSH BINARY LOGS;")
		require.NoError(t, err)
	}

	type binlogRow struct {
		LogName  string
		FileSize uint64
	}
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS;")
	require.NoError(t, err)
	defer rows.Close()
	var logs []binlogRow
	for rows.Next() {
		var name string
		var size uint64
		require.NoError(t, rows.Scan(&name, &size))
		logs = append(logs, binlogRow{LogName: name, FileSize: size})
	}
	require.GreaterOrEqual(t, len(logs), 2)

	// Выберем самый ранний и текущий следующий
	earliest := logs[0].LogName
	purgeTo := logs[len(logs)-1].LogName // purge до текущего, чтобы earliest убрался

	_, err = db.ExecContext(ctx, fmt.Sprintf("PURGE BINARY LOGS TO '%s';", purgeTo))
	require.NoError(t, err)

	fakeCp := coordinator.NewStatefulFakeClient()
	tr, err := pmysql.NewTracker(src, "test-transfer-id", fakeCp)
	require.NoError(t, err)
	require.NoError(t, tr.Store(earliest, 4))

	transfer := &model.Transfer{ID: "test-transfer-id", Src: src, Dst: &model.MockDestination{}}
	src.WithDefaults()
	provider := pmysql.New(logger.Log, nil, coordinator.NewFakeClient(), transfer)

	// Пробуем создать Source и запустить publisher
	registry := solomon.NewRegistry(solomon.NewRegistryOpts())
	s, err := pmysql.NewSource(src, transfer.ID, &model.DataObjects{}, logger.Log, registry, fakeCp, true)
	require.NoError(t, err)
	// Запускаем source с no-op sink'ом; ошибка должна вернуться при старте канала
	err = s.Run(&noopAsyncSink{})
	require.Error(t, err)
	if !codes.MySQLBinlogFirstFileMissing.Contains(err) {
		t.Fatalf("expected codes.MySQLBinlogFirstFileMissing, got: %v", err)
	}
	_ = provider // silence unused in case of build variants
}
