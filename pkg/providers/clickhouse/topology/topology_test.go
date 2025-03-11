package topology

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
)

func TestClusterName(t *testing.T) {
	t.Run("TestResolveFromConfig", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		cfgRaw := &model.ChDestination{ChClusterName: "foo"}
		cfgRaw.WithDefaults()
		cfg := cfgRaw.ToReplicationFromPGSinkParams()

		name, err := resolveClusterName(context.Background(), db, cfg)
		require.NoError(t, err)
		require.Equal(t, "foo", name)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("TestResolveFromMDB", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		cfgRaw := &model.ChDestination{MdbClusterID: "mdb123"}
		cfgRaw.WithDefaults()
		cfg := cfgRaw.ToReplicationFromPGSinkParams()

		mockRows := sqlmock.NewRows([]string{"cluster"}).FromCSVString("foo\n")
		mock.ExpectQuery(`select substitution from system.macros where macro = 'cluster';`).
			WillReturnRows(mockRows).RowsWillBeClosed()
		name, err := resolveClusterName(context.Background(), db, cfg)
		require.NoError(t, err)
		require.Equal(t, "foo", name)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("TestResolveOnPremFromSystemTable", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		cfgRaw := &model.ChDestination{}
		cfgRaw.WithDefaults()
		cfg := cfgRaw.ToReplicationFromPGSinkParams()

		mockRows := sqlmock.NewRows([]string{"cluster"}).FromCSVString("foo\n")
		mock.ExpectQuery(`select cluster from system.clusters limit 1;`).
			WillReturnRows(mockRows).RowsWillBeClosed()
		name, err := resolveClusterName(context.Background(), db, cfg)
		require.NoError(t, err)
		require.Equal(t, "foo", name)
	})

	t.Run("TestResolveOnPremFromEmptySystemTable", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		cfgRaw := &model.ChDestination{}
		cfgRaw.WithDefaults()
		cfg := cfgRaw.ToReplicationFromPGSinkParams()

		mockRows := sqlmock.NewRows([]string{"cluster"})
		mock.ExpectQuery(`select cluster from system.clusters limit 1;`).
			WillReturnRows(mockRows).RowsWillBeClosed()
		name, err := resolveClusterName(context.Background(), db, cfg)
		require.ErrorIs(t, err, ErrNoCluster)
		require.Equal(t, "", name)
	})
}
