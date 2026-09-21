package tests

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
)

func TestShardingStorage_IncrementalTable(t *testing.T) {
	_ = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("test_scripts"))
	srcPort, _ := strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	v := &provider_postgres.PgSource{
		Hosts:    []string{"localhost"},
		User:     os.Getenv("PG_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database: os.Getenv("PG_LOCAL_DATABASE"),
		Port:     srcPort,
	}
	v.WithDefaults()
	require.NotEqual(t, 0, v.DesiredTableSize)
	storage, err := provider_postgres.NewStorage(v.ToStorageParams(nil))
	require.NoError(t, err)

	err = storage.BeginPGSnapshot(context.TODO())
	require.NoError(t, err)
	logger.Log.Infof("create snapshot: %v", storage.ShardedStateLSN)

	t.Run("incremental numeric", func(t *testing.T) {
		res, err := storage.GetNextIncrementalState(context.TODO(), []abstract.IncrementalTable{{
			Name:        "__test_incremental",
			Namespace:   "public",
			CursorField: "cursor",
		}})
		require.NoError(t, err)
		require.NotNil(t, res)
		_, err = storage.Conn.Exec(context.TODO(), `
insert into __test_incremental (text, cursor)
select md5(random()::text), s.s from generate_Series(
	(select max(cursor) from __test_incremental),
	(select max(cursor) + 10 from __test_incremental)
) as s;
`)
		require.NoError(t, err)
		require.Len(t, res, 1)

		storage.ShardedStateLSN = ""
		var incrementRes []abstract.ChangeItem
		for _, tdesc := range abstract.IncrementalStateToTableDescription(res) {
			require.NoError(t, storage.LoadTable(context.Background(), tdesc, func(input []abstract.ChangeItem) error {
				for _, row := range input {
					if row.IsRowEvent() {
						incrementRes = append(incrementRes, row)
					}
				}
				return nil
			}))
		}
		logger.Log.Infof("count: %v", len(incrementRes))
		require.Len(t, incrementRes, 10)
	})
	t.Run("incremental timestamp", func(t *testing.T) {
		res, err := storage.GetNextIncrementalState(context.TODO(), []abstract.IncrementalTable{{
			Name:        "__test_incremental_ts",
			Namespace:   "public",
			CursorField: "cursor",
		}})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, res, 1)
		_, err = storage.Conn.Exec(context.TODO(), `
insert into __test_incremental_ts (text, cursor)
select md5(random()::text), $1 from generate_Series(1,10) as s;
`, time.Now().UTC())
		require.NoError(t, err)

		storage.ShardedStateLSN = ""
		var incrementRes []abstract.ChangeItem
		for _, tdesc := range abstract.IncrementalStateToTableDescription(res) {
			require.NoError(t, storage.LoadTable(context.Background(), tdesc, func(input []abstract.ChangeItem) error {
				for _, row := range input {
					if row.IsRowEvent() {
						incrementRes = append(incrementRes, row)
					}
				}
				return nil
			}))
		}
		logger.Log.Infof("count: %v", len(incrementRes))
		require.Len(t, incrementRes, 10)
	})
	t.Run("cursor type is read from catalog for empty table", func(t *testing.T) {
		_, err := storage.GetNextIncrementalState(context.TODO(), []abstract.IncrementalTable{{
			Name:         "__test_incremental_empty",
			Namespace:    "public",
			CursorField:  "cursor",
			InitialState: "'not-an-integer'",
		}})
		require.ErrorContains(t, err, "unable get max cursor")
		require.ErrorContains(t, err, "invalid input syntax")
		require.ErrorContains(t, err, `integer: "not-an-integer"`)
	})
}

func TestStorage_IncrementalInitialState(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("test_scripts"))
	storage, err := provider_postgres.NewStorage(src.ToStorageParams(nil))
	require.NoError(t, err)
	t.Cleanup(storage.Close)

	ctx := context.Background()
	_, err = storage.Conn.Exec(ctx, `
CREATE TABLE __test_incremental_initial_state (
	cursor_ts timestamp,
	cursor_tstz timestamp with time zone,
	cursor_int integer,
	cursor_text text
);
INSERT INTO __test_incremental_initial_state VALUES
	('2022-12-31', '2022-12-31 00:00:00+00', 1, 'a'),
	('2023-01-02', '2023-01-02 00:00:00+00', 2, 'b'),
	(NULL, NULL, NULL, NULL);
`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := storage.Conn.Exec(ctx, `DROP TABLE __test_incremental_initial_state`)
		require.NoError(t, err)
	})

	for _, tc := range []struct {
		name         string
		cursorField  string
		initialState string
		stateCount   int
	}{
		{"to_date for timestamptz", "cursor_tstz", "to_date('2023-01-01', 'YYYY-MM-DD')", 1},
		{"timestamp literal", "cursor_ts", "timestamp'2000-03-16'", 1},
		{"quoted timestamp", "cursor_ts", "'2023-01-01'", 1},
		{"numeric literal", "cursor_int", "1", 1},
		{"numeric expression", "cursor_int", "1 + 0", 1},
		{"text literal", "cursor_text", "'a'", 1},
		{"timestamp at maximum", "cursor_ts", "timestamp'2023-01-02'", 0},
		{"timestamptz above maximum", "cursor_tstz", "to_date('2023-01-03', 'YYYY-MM-DD')", 0},
		{"numeric at maximum", "cursor_int", "1 + 1", 0},
		{"no initial state with nulls", "cursor_ts", "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := storage.GetNextIncrementalState(ctx, []abstract.IncrementalTable{{
				Name:         "__test_incremental_initial_state",
				Namespace:    "public",
				CursorField:  tc.cursorField,
				InitialState: tc.initialState,
			}})
			require.NoError(t, err)
			require.Len(t, res, tc.stateCount)
			if tc.stateCount > 0 {
				var count int
				err = storage.Conn.QueryRow(ctx, `SELECT count(*) FROM __test_incremental_initial_state WHERE NOT (`+string(res[0].Payload)+`)`).Scan(&count)
				require.NoError(t, err)
				require.Equal(t, 2, count)
			}
		})
	}
}

func TestInitialStatePopulate(t *testing.T) {
	srcPort, _ := strconv.Atoi(os.Getenv("SOURCE_PG_LOCAL_PORT"))
	v := &provider_postgres.PgSource{
		Hosts:    []string{"localhost"},
		User:     os.Getenv("SOURCE_PG_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("SOURCE_PG_LOCAL_PASSWORD")),
		Database: os.Getenv("SOURCE_PG_LOCAL_DATABASE"),
		Port:     srcPort,
	}
	v.WithDefaults()
	require.NotEqual(t, 0, v.DesiredTableSize)
	storage := new(provider_postgres.Storage)

	t.Run("single table", func(t *testing.T) {
		tables := []abstract.TableDescription{{
			Name:   "foo",
			Schema: "bar",
			Filter: "",
			EtaRow: 0,
			Offset: 0,
		}}
		incremental := []abstract.IncrementalTable{{
			Name:         "foo",
			Namespace:    "bar",
			CursorField:  "buzz",
			InitialState: "'fuzz'",
		}}
		tablesOut := storage.BuildArrTableDescriptionWithIncrementalState(tables, incremental)
		require.Equal(t, tablesOut[0].Filter, abstract.WhereStatement(`"buzz" > 'fuzz'`))
	})
	t.Run("single table not match", func(t *testing.T) {
		tables := []abstract.TableDescription{{
			Name:   "foo",
			Schema: "bar",
			Filter: "",
			EtaRow: 0,
			Offset: 0,
		}}
		incremental := []abstract.IncrementalTable{{
			Name:         "foo",
			Namespace:    "bar_not_match",
			CursorField:  "buzz",
			InitialState: "'fuzz'",
		}}
		tablesOut := storage.BuildArrTableDescriptionWithIncrementalState(tables, incremental)
		require.Equal(t, tablesOut[0].Filter, abstract.WhereStatement(``))
	})
	t.Run("many tables one match", func(t *testing.T) {
		tables := []abstract.TableDescription{{
			Name:   "foo",
			Schema: "bar-other",
			Filter: "",
			EtaRow: 0,
			Offset: 0,
		}, {
			Name:   "foo",
			Schema: "bar",
			Filter: "",
			EtaRow: 0,
			Offset: 0,
		}}
		incremental := []abstract.IncrementalTable{{
			Name:         "foo",
			Namespace:    "bar",
			CursorField:  "buzz",
			InitialState: "'fuzz'",
		}}
		tablesOut := storage.BuildArrTableDescriptionWithIncrementalState(tables, incremental)
		require.Equal(t, tablesOut[0].Filter, abstract.WhereStatement(``))
		require.Equal(t, tablesOut[1].Filter, abstract.WhereStatement(`"buzz" > 'fuzz'`))
	})
	t.Run("partial upload", func(t *testing.T) {
		tables := []abstract.TableDescription{{
			Name:   "foo",
			Schema: "bar-other",
			Filter: "exist-filter",
			EtaRow: 0,
			Offset: 0,
		}}
		incremental := []abstract.IncrementalTable{{
			Name:         "foo",
			Namespace:    "bar",
			CursorField:  "buzz",
			InitialState: "'fuzz'",
		}}
		tablesOut := storage.BuildArrTableDescriptionWithIncrementalState(tables, incremental)
		require.Equal(t, tablesOut[0].Filter, abstract.WhereStatement(`exist-filter`))
	})
}
