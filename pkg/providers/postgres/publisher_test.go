//go:build !disable_postgres_provider

package postgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres/dblog"
)

func TestWal2jsonTableFromTableID(t *testing.T) {
	t.Run("wal2jsonfti_schema_table", func(t *testing.T) {
		require.Equal(
			t,
			`schema.table`,
			wal2jsonTableFromTableID(*abstract.NewTableID("schema", "table")),
		)
	})

	t.Run("wal2jsonfti_noschema_table", func(t *testing.T) {
		require.Equal(
			t,
			`*.table`,
			wal2jsonTableFromTableID(*abstract.NewTableID("", "table")),
		)
	})

	t.Run("wal2jsonfti_schema_star", func(t *testing.T) {
		require.Equal(
			t,
			`schema.*`,
			wal2jsonTableFromTableID(*abstract.NewTableID("schema", "*")),
		)
	})

	t.Run("wal2jsonfti_noschema_star", func(t *testing.T) {
		require.Equal(
			t,
			`*.*`,
			wal2jsonTableFromTableID(*abstract.NewTableID("", "*")),
		)
	})

	t.Run("wal2jsonfti_schema_tablewithdots", func(t *testing.T) {
		require.Equal(
			t,
			`schema.tab\.l\.e`,
			wal2jsonTableFromTableID(*abstract.NewTableID("schema", "tab.l.e")),
		)
	})

	t.Run("wal2jsonfti_schemawithdots_table", func(t *testing.T) {
		require.Equal(
			t,
			`sche\.ma\..table`,
			wal2jsonTableFromTableID(*abstract.NewTableID("sche.ma.", "table")),
		)
	})

	t.Run("wal2jsonfti_schemawithdots_notable", func(t *testing.T) {
		require.Equal(
			t,
			`sche\.ma\..*`,
			wal2jsonTableFromTableID(*abstract.NewTableID("sche.ma.", "")),
		)
	})
}

func TestNewWal2jsonArgumentsDBLog(t *testing.T) {
	extractValue := func(in []argument) string {
		for _, t := range in {
			if t.name == "add-tables" {
				return t.value
			}
		}
		return ""
	}
	isIncludesSignalTable := func(in []argument, cfg *PgSource) bool {
		addTablesValue := extractValue(in)
		signalTableID := *dblog.SignalTableTableID(cfg.KeeperSchema)
		signalTableIDStr := signalTableID.Namespace + "." + signalTableID.Name
		return strings.Contains(addTablesValue, signalTableIDStr)
	}

	t.Run("dblog snapshot", func(t *testing.T) {
		cfg := &PgSource{}
		cfg.KeeperSchema = "public"
		cfg.DBLogEnabled = true
		cfg.DBTables = []string{"public.my_table"}
		wal2jsonArguments, err := newWal2jsonArguments(cfg, nil, true)
		require.NoError(t, err)
		require.True(t, isIncludesSignalTable(wal2jsonArguments, cfg))
	})

	t.Run("dblog replication", func(t *testing.T) {
		cfg := &PgSource{}
		cfg.KeeperSchema = "public"
		cfg.DBLogEnabled = true
		cfg.DBTables = []string{"public.my_table"}
		wal2jsonArguments, err := newWal2jsonArguments(cfg, nil, false)
		require.NoError(t, err)
		require.False(t, isIncludesSignalTable(wal2jsonArguments, cfg))
	})

	t.Run("not-dblog snapshot", func(t *testing.T) {
		cfg := &PgSource{}
		cfg.KeeperSchema = "public"
		cfg.DBLogEnabled = false
		cfg.DBTables = []string{"public.my_table"}
		wal2jsonArguments, err := newWal2jsonArguments(cfg, nil, false)
		require.NoError(t, err)
		require.False(t, isIncludesSignalTable(wal2jsonArguments, cfg))
	})

	t.Run("not-dblog replication", func(t *testing.T) {
		cfg := &PgSource{}
		cfg.KeeperSchema = "public"
		cfg.DBLogEnabled = false
		cfg.DBTables = []string{"public.my_table"}
		wal2jsonArguments, err := newWal2jsonArguments(cfg, nil, false)
		require.NoError(t, err)
		require.False(t, isIncludesSignalTable(wal2jsonArguments, cfg))
	})
}
