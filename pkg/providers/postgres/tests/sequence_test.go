//go:build !disable_postgres_provider

package tests

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
)

func connect(ctx context.Context, t *testing.T) *pgxpool.Pool {
	poolConfig, err := pgxpool.ParseConfig("")
	require.NoError(t, err)
	connConfig := poolConfig.ConnConfig
	if host, ok := os.LookupEnv("PG_LOCAL_HOST"); ok {
		connConfig.Host = host
	} else {
		connConfig.Host = "localhost"
	}
	port, err := strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	require.NoError(t, err)
	connConfig.Port = uint16(port)
	connConfig.Database = os.Getenv("PG_LOCAL_DATABASE")
	connConfig.User = os.Getenv("PG_LOCAL_USER")
	connConfig.Password = os.Getenv("PG_LOCAL_PASSWORD")
	if certPath, ok := os.LookupEnv("PG_LOCAL_CERT"); ok {
		certFile, err := os.ReadFile(certPath)
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(certFile)
		require.NoError(t, err)
		connConfig.TLSConfig = &tls.Config{
			ServerName: connConfig.Host,
			RootCAs:    certPool,
		}
	}
	pool, err := pgxpool.ConnectConfig(ctx, poolConfig)
	require.NoError(t, err)
	return pool
}

func TestListSequencesInParallel(t *testing.T) {
	if os.Getenv("USE_TESTCONTAINERS") == "1" {
		t.Skip()
	}
	_ = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("test_scripts"))
	ctx := context.Background()
	pool := connect(ctx, t)
	defer pool.Close()

	txOptions := pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable}

	testListSequences := func(tx pgx.Tx) {
		_, err := postgres.ListSequencesWithDependants(ctx, tx.Conn(), "public")
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		fmt.Println("open tx")
		tx, err := pool.BeginTx(ctx, txOptions)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, tx.Rollback(ctx))
		}()
		testListSequences(tx)
	}
}

func TestListSequences(t *testing.T) {
	_ = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("test_scripts"))
	ctx := context.Background()
	pool := connect(ctx, t)
	defer pool.Close()

	txOptions := pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable}
	tx, err := pool.BeginTx(ctx, txOptions)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tx.Rollback(ctx))
	}()

	sequences, err := postgres.ListSequencesWithDependants(ctx, tx.Conn(), "public")
	require.NoError(t, err)

	seq, ok := sequences[*abstract.NewTableID("public", "test_table_with_serial_id_seq")]
	require.True(t, ok)
	require.Equal(t, []abstract.TableID{*abstract.NewTableID("public", "test_table_with_serial")}, seq.DependentTables)

	seq, ok = sequences[*abstract.NewTableID("public", "test_seq")]
	sort.Slice(seq.DependentTables, func(i, j int) bool {
		l := seq.DependentTables[i]
		r := seq.DependentTables[j]
		if l.Namespace < r.Namespace {
			return true
		} else if l.Namespace > r.Namespace {
			return false
		}
		return l.Name < r.Name
	})
	require.True(t, ok)
	require.Equal(t, []abstract.TableID{
		*abstract.NewTableID("public", "test_table_with_default1"),
		*abstract.NewTableID("public", "test_table_with_default2"),
	}, seq.DependentTables)

	seq, ok = sequences[*abstract.NewTableID("public", "test_seq_without_dependents")]
	require.True(t, ok)
	require.Equal(t, 0, len(seq.DependentTables))

	seq, ok = sequences[*abstract.NewTableID("public", "gbdai_seq")]
	require.True(t, ok)
	require.Equal(t, []abstract.TableID{
		*abstract.NewTableID("public", "table_with_gbdai_seq"),
	}, seq.DependentTables)
	// check value of the last sequence
	lastValue, isCalled, err := postgres.GetCurrentStateOfSequence(ctx, tx.Conn(), seq.SequenceID)
	require.NoError(t, err)
	require.Equal(t, int64(3), lastValue)
	require.True(t, isCalled)
}
