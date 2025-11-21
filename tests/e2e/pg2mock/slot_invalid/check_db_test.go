package main

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	proxy "github.com/transferia/transferia/tests/helpers/proxies/pg_proxy"
)

func TestPollingFailsOnSlotInvalidation(t *testing.T) {
	t.Setenv("YC", "1") // avoid trying to talk to production control plane

	source := pgrecipe.RecipeSource(
		pgrecipe.WithInitDir("init_source"),
		pgrecipe.WithEdit(func(pg *pgcommon.PgSource) {
			pg.DBTables = []string{"public.__test1"}
			pg.UsePolling = true
		}),
	)

	originalPort := source.Port
	listenPort := originalPort + 1
	listenAddr := net.JoinHostPort(source.Hosts[0], strconv.Itoa(listenPort))
	postgresAddr := net.JoinHostPort(source.Hosts[0], strconv.Itoa(originalPort))

	slotProxy := proxy.NewProxy(listenAddr, postgresAddr)
	slotProxy.AddErrorHandler(
		"pg_logical_slot_peek_binary_changes",
		&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "55000",
			Message:  "replication slot invalidated by test proxy",
		},
	)
	slotProxy.Start()
	waitForProxy(t, listenAddr)

	defer func() {
		require.NoError(
			t,
			helpers.CheckConnections(
				helpers.LabeledPort{Label: "PG source proxy", Port: listenPort},
			),
		)
	}()
	defer slotProxy.Close()

	source.Port = listenPort

	transfer := helpers.MakeTransfer(
		helpers.GenerateTransferID(t.Name()),
		source,
		&model.MockDestination{
			SinkerFactory: func() abstract.Sinker {
				return &helpers.MockSink{
					PushCallback: func([]abstract.ChangeItem) error { return nil },
				}
			},
			Cleanup: model.DisabledCleanup,
		},
		abstract.TransferTypeIncrementOnly,
	)

	fatalErrCh := make(chan error, 1)

	worker := helpers.Activate(t, transfer, func(err error) {
		fatalErrCh <- err
	})
	defer func() {
		require.NoError(t, worker.CloseWithErr())
	}()

	select {
	case err := <-fatalErrCh:
		require.Error(t, err)
		require.True(t, abstract.IsFatal(err))
		require.Contains(t, err.Error(), "55000")
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for fatal polling error")
	}
}

func waitForProxy(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("proxy %s is not ready", addr)
}
