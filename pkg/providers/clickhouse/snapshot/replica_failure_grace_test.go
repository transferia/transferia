package snapshot

import (
	"context"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/part_uploader"
)

func TestReplicaFailureGraceSurvivesPartSinkRecreation(test *testing.T) {
	primaryDB, primary, err := sqlmock.New()
	require.NoError(test, err)
	defer primaryDB.Close()
	secondaryDB, secondary, err := sqlmock.New()
	require.NoError(test, err)
	defer secondaryDB.Close()

	transferID := test.Name()
	const operationID = "operation"
	test.Cleanup(func() { StopStagingDDLKeepalives(transferID, operationID) })
	nodes := [][]ch_eo_common.Node{{
		ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "primary"}, primaryDB, newSessionHTTPClient(func(url.Values) error { return nil })),
		ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "secondary"}, secondaryDB, newSessionHTTPClient(func(url.Values) error { return nil })),
	}}
	makeSink := func() *Sink {
		uploader := part_uploader.NewUploader(1, "test", false, nil, nil)
		test.Cleanup(func() { require.NoError(test, uploader.Close()) })
		sink, err := newSink(
			context.Background(),
			logger.Log,
			&clickhouse_model.ChDestination{Database: "test", Cleanup: model.Drop},
			newSerialState(),
			transferID,
			operationID,
			nodes,
			false,
			nil,
			nil,
			uploader,
		)
		require.NoError(test, err)
		// The production factory binds loading sinks to the same operation history.
		sink.tablesClient.SetReplicaFailures(stagingDDLSessions.replicaFailures(sink.stagingDDLOperationKey(operationID)))
		sink.tablesClient.SetReplicaFailureGrace(100 * time.Millisecond)
		return sink
	}
	firstSink := makeSink()
	primary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	for probe := 0; probe < 2; probe++ {
		secondary.ExpectQuery("SELECT toUInt8").WillReturnError(io.ErrUnexpectedEOF)
	}
	available, err := firstSink.tablesClient.SelectReplicas(context.Background(), true)
	require.NoError(test, err)
	require.Len(test, available[0], 1)

	// A later part still probes the offline host, but its original grace has elapsed.
	secondSink := makeSink()
	primary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	secondary.ExpectQuery("SELECT toUInt8").WillReturnError(io.ErrUnexpectedEOF)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	available, err = secondSink.tablesClient.SelectReplicas(ctx, true)
	require.NoError(test, err)
	require.Len(test, available[0], 1)
	require.NoError(test, primary.ExpectationsWereMet())
	require.NoError(test, secondary.ExpectationsWereMet())
}
