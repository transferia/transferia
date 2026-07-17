package column_defaults

import (
	"context"
	_ "embed"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/oracle/oraclerecipe"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
)

//go:embed dump/init.sql
var initSQL string

var (
	TransferType = abstract.TransferTypeSnapshotOnly

	Source = *oraclerecipe.RecipeOracleSource()

	dstPort, _ = strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	Target     = provider_postgres.PgDestination{
		Hosts:    []string{"localhost"},
		User:     os.Getenv("PG_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database: os.Getenv("PG_LOCAL_DATABASE"),
		Port:     dstPort,
		Cleanup:  model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

func TestColumnDefaults(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Oracle source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("ColumnDefaults", ColumnDefaults)
	})
}

func ColumnDefaults(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.DEFAULTS_TEST"}
	Source.ConvertNumberToInt64 = true

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)

	// Phase 1: verify abstract schema carries default properties.
	pgStorage := helpers.GetSampleableStorageByModel(t, &Target)
	schema, err := pgStorage.TableSchema(context.Background(), *abstract.NewTableID("dt_test", "defaults_test"))
	require.NoError(t, err)

	defaultProps := map[string]string{}
	for _, col := range schema.Columns() {
		if d, ok := col.Properties[changeitem.DefaultPropertyKey]; ok {
			defaultProps[col.ColumnName] = d.(string)
		}
	}

	require.Equal(t, "42", defaultProps["col_int"], "col_int default")
	require.Equal(t, "'ACTIVE'", defaultProps["col_varchar"], "col_varchar default")
	require.Equal(t, "now()", defaultProps["col_date_sysdate"], "col_date_sysdate default")
	require.Equal(t, "now()", defaultProps["col_ts_systime"], "col_ts_systime default")
	require.Equal(t, "CURRENT_TIMESTAMP", defaultProps["col_cts"], "col_cts default")
	require.Equal(t, "CURRENT_DATE", defaultProps["col_curdate"], "col_curdate default")

	_, hasNoDefault := defaultProps["col_no_default"]
	require.False(t, hasNoDefault, "col_no_default must not have default")
	_, hasNullDefault := defaultProps["col_null_default"]
	require.False(t, hasNullDefault, "col_null_default must not have default")

	// Phase 2: verify PG information_schema column_default.
	pool, err := provider_postgres.MakeConnPoolFromDst(&Target, logger.Log)
	require.NoError(t, err)
	defer pool.Close()

	rows, err := pool.Query(context.Background(), `
		SELECT column_name, column_default
		FROM information_schema.columns
		WHERE table_schema = 'dt_test' AND table_name = 'defaults_test'
		ORDER BY ordinal_position`)
	require.NoError(t, err)
	defer rows.Close()

	pgDefaults := map[string]*string{}
	for rows.Next() {
		var colName string
		var colDefault *string
		require.NoError(t, rows.Scan(&colName, &colDefault))
		pgDefaults[colName] = colDefault
	}
	require.NoError(t, rows.Err())

	// Numeric and string literal defaults.
	require.NotNil(t, pgDefaults["col_int"])
	require.Equal(t, "42", *pgDefaults["col_int"])
	require.NotNil(t, pgDefaults["col_varchar"])
	require.Contains(t, *pgDefaults["col_varchar"], "'ACTIVE'")

	// Oracle function defaults mapped to PG.
	require.NotNil(t, pgDefaults["col_date_sysdate"])
	require.NotNil(t, pgDefaults["col_ts_systime"])
	require.Equal(t, "CURRENT_TIMESTAMP", *pgDefaults["col_cts"])
	require.Equal(t, "CURRENT_DATE", *pgDefaults["col_curdate"])

	// No-default and NULL-default columns.
	require.Nil(t, pgDefaults["col_no_default"])
	require.Nil(t, pgDefaults["col_null_default"])

	// Phase 3: verify defaults work at INSERT time.
	_, err = pool.Exec(context.Background(),
		`INSERT INTO dt_test.defaults_test (id) VALUES (3)`)
	require.NoError(t, err)

	var colInt int
	var colVarchar string
	err = pool.QueryRow(context.Background(),
		`SELECT col_int, col_varchar FROM dt_test.defaults_test WHERE id = 3`,
	).Scan(&colInt, &colVarchar)
	require.NoError(t, err)
	require.Equal(t, 42, colInt)
	require.Equal(t, "ACTIVE", colVarchar)

	// Date/time defaults must be non-null (value is non-deterministic but not null).
	var colDate, colTS, colCurDate *time.Time
	err = pool.QueryRow(context.Background(),
		`SELECT col_date_sysdate, col_ts_systime, col_curdate FROM dt_test.defaults_test WHERE id = 3`,
	).Scan(&colDate, &colTS, &colCurDate)
	require.NoError(t, err)
	require.NotNil(t, colDate)
	require.NotNil(t, colTS)
	require.NotNil(t, colCurDate)

	// Phase 4: verify explicit values from row 1 are preserved.
	var explicitVarchar string
	err = pool.QueryRow(context.Background(),
		`SELECT col_varchar FROM dt_test.defaults_test WHERE id = 1`,
	).Scan(&explicitVarchar)
	require.NoError(t, err)
	require.Equal(t, "EXPLICIT", explicitVarchar)

	// Row count: 2 from init.sql + 1 from Phase 3 = 3.
	helpers.CheckRowsCount(t, &Target, "dt_test", "defaults_test", 3)
}
