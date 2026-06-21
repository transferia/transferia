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

func TestWal2jsonArgumentsFiltersServiceTables(t *testing.T) {
	arg := func(name string, args []argument) string {
		for _, a := range args {
			if a.name == name {
				return a.value
			}
		}
		return ""
	}

	for _, testCase := range []struct {
		name                 string
		DBTables             []string
		ExcludedTables       []string
		dbLogSnapshot        bool
		expectedAddTables    []string
		expectedFilterTables []string
	}{
		{
			name:                 "ignore excluded system tables",
			DBTables:             []string{"public.my_table"},
			ExcludedTables:       []string{"public.__consumer_keeper"},
			dbLogSnapshot:        false,
			expectedAddTables:    []string{"public.my_table", "public.__consumer_keeper"},
			expectedFilterTables: []string{},
		},
		{
			name:                 "ignore excluded signal table for dblog snapshot and not included tables",
			DBTables:             []string{},
			ExcludedTables:       []string{"public.__consumer_keeper", "public.__data_transfer_signal_table"},
			dbLogSnapshot:        true,
			expectedAddTables:    []string{},
			expectedFilterTables: []string{},
		},
		{
			name:                 "ignore excluded signal table for dblog snapshot and included tables",
			DBTables:             []string{"public.my_table"},
			ExcludedTables:       []string{"public.__consumer_keeper", "public.__data_transfer_signal_table"},
			dbLogSnapshot:        true,
			expectedAddTables:    []string{"public.my_table", "public.__consumer_keeper", "public.__data_transfer_signal_table"},
			expectedFilterTables: []string{},
		},
		{
			name:                 "exclude signal table for not dblog snapshot",
			DBTables:             []string{"public.my_table"},
			ExcludedTables:       []string{"public.__data_transfer_signal_table"},
			dbLogSnapshot:        false,
			expectedAddTables:    []string{"public.my_table", "public.__consumer_keeper"},
			expectedFilterTables: []string{"public.__data_transfer_signal_table"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := &PgSource{
				KeeperSchema:   "public",
				DBTables:       testCase.DBTables,
				ExcludedTables: testCase.ExcludedTables,
			}
			wal2jsonArguments, err := newWal2jsonArguments(cfg, nil, testCase.dbLogSnapshot)
			require.NoError(t, err)
			require.Equal(t, strings.Join(testCase.expectedAddTables, ","), arg("add-tables", wal2jsonArguments))
			alwaysExclude := []string{"public.repl_mon"}
			withGlobalExclude := append(testCase.expectedFilterTables, alwaysExclude...)
			require.Equal(t, strings.Join(withGlobalExclude, ","), arg("filter-tables", wal2jsonArguments))
		})
	}
}

func TestIsTableOrParentIncludedCollapseInherit(t *testing.T) {
	parent := abstract.TableID{Namespace: "public", Name: "parent"}
	child := abstract.TableID{Namespace: "public", Name: "child"}
	other := abstract.TableID{Namespace: "public", Name: "other"}
	cfg := &PgSource{
		DBTables:              []string{parent.Fqtn()},
		CollapseInheritTables: true,
		KeeperSchema:          "public",
	}
	altNames := map[abstract.TableID]abstract.TableID{
		child: parent,
	}

	require.True(t, isTableOrParentIncluded(altNames, parent, cfg, true))
	require.True(t, isTableOrParentIncluded(altNames, child, cfg, true))
	require.False(t, isTableOrParentIncluded(altNames, other, cfg, true))
}

func TestCountPublisherMetrics(t *testing.T) {
	userInsert := abstract.ChangeItem{
		Kind:   abstract.InsertKind,
		Schema: "public",
		Table:  "user_table",
	}
	userUpdate := abstract.ChangeItem{
		Kind:   abstract.UpdateKind,
		Schema: "public",
		Table:  "user_table",
	}
	systemInsert := abstract.ChangeItem{
		Kind:   abstract.InsertKind,
		Schema: "public",
		Table:  TableConsumerKeeper,
	}
	systemDDL := abstract.ChangeItem{
		Kind:   abstract.InitTableLoad,
		Schema: "public",
		Table:  TableConsumerKeeper,
	}

	t.Run("mixed batch excludes system tables", func(t *testing.T) {
		changeItems, parsedRows := countPublisherMetrics([]abstract.ChangeItem{
			userInsert,
			systemInsert,
			userUpdate,
			systemDDL,
		})

		require.EqualValues(t, 2, changeItems)
		require.EqualValues(t, 2, parsedRows)
	})

	t.Run("system only batch does not affect metrics", func(t *testing.T) {
		changeItems, parsedRows := countPublisherMetrics([]abstract.ChangeItem{
			systemInsert,
			systemDDL,
		})

		require.Zero(t, changeItems)
		require.Zero(t, parsedRows)
	})
}
