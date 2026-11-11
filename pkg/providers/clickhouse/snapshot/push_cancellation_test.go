package snapshot

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

func newBlockedStagingCreateSink(test *testing.T, ctx context.Context) (*Sink, <-chan struct{}, <-chan struct{}, func()) {
	test.Helper()
	createStarted := make(chan struct{})
	createCanceled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCreate := func() { releaseOnce.Do(func() { close(release) }) }
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		decoder, err := zstd.NewReader(request.Body)
		if err != nil {
			test.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		statement, err := io.ReadAll(decoder)
		decoder.Close()
		if err != nil {
			test.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if !strings.HasPrefix(string(statement), "CREATE TABLE ") {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		close(createStarted)
		select {
		case <-request.Context().Done():
			close(createCanceled)
		case <-release:
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(writer, "Code: 999. DB::Exception: released test CREATE")
		}
	}))
	test.Cleanup(server.Close)
	test.Cleanup(releaseCreate)
	address := server.Listener.Addr().(*net.TCPAddr)
	host := &conn_clickhouse.Host{Name: address.IP.String(), HTTPPort: address.Port}
	config := &clickhouse_model.ChDestination{Database: "test", Cleanup: model.Drop}
	params, err := config.ToSinkParams(new(model.Transfer))
	require.NoError(test, err)
	client, err := httpclient.NewHTTPClientImpl(params)
	require.NoError(test, err)
	test.Cleanup(client.CloseIdleConnections)
	database, queries, err := sqlmock.New()
	require.NoError(test, err)
	queries.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	queries.ExpectQuery("SELECT engine FROM system.databases").WithArgs("test").WillReturnRows(sqlmock.NewRows([]string{"engine"}).AddRow("Atomic"))
	queries.ExpectQuery("SELECT name FROM system.tables").WillReturnRows(sqlmock.NewRows([]string{"name"}))
	queries.ExpectClose()
	nodes := [][]ch_eo_common.Node{{ch_eo_common.NewNode(host, database, client)}}
	uploader, err := newTestUploader(nodes, config.Database, &testUploadFixture{sharder: sharding.ConstSharder()})
	require.NoError(test, err)
	sink, err := newSink(
		ctx,
		logger.Log,
		config,
		newSerialState(),
		"transfer",
		"operation",
		nodes,
		false,
		nil,
		nil,
		uploader,
	)
	require.NoError(test, err)
	test.Cleanup(func() {
		releaseCreate()
		require.NoError(test, sink.Close())
		require.NoError(test, queries.ExpectationsWereMet())
	})
	tableID := abstract.TableID{Namespace: "source", Name: "events"}
	table := sink.publicationStore.NewTable(tableID, "events", "CREATE TABLE events (id UInt64) ENGINE = MergeTree() ORDER BY id", false)
	require.NoError(test, table.RegisterStaging(context.Background(), "__cancellation_", map[string]string{host.Name: "staging-session"}))
	return sink, createStarted, createCanceled, releaseCreate
}

func TestSinkCloseCancelsStagingCreate(test *testing.T) {
	sink, createStarted, createCanceled, _ := newBlockedStagingCreateSink(test, context.Background())
	pushDone := make(chan error, 1)
	go func() {
		pushDone <- sink.Push([]abstract.ChangeItem{{Kind: abstract.InitTableLoad, Schema: "source", Table: "events", PartID: "part"}})
	}()
	select {
	case <-createStarted:
	case err := <-pushDone:
		test.Fatalf("snapshot stopped before staging CREATE: %v", err)
	case <-time.After(5 * time.Second):
		test.Fatal("snapshot did not reach staging CREATE")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- sink.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(test, err)
	case <-time.After(2 * time.Second):
		test.Fatal("Sink.Close did not interrupt the staging CREATE")
	}
	require.ErrorIs(test, <-pushDone, context.Canceled)
	select {
	case <-createCanceled:
	case <-time.After(2 * time.Second):
		test.Fatal("staging HTTP request was not canceled")
	}
}
