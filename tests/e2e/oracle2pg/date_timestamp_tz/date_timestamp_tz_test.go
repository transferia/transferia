package date_timestamp_tz

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	testcontainers_go "github.com/testcontainers/testcontainers-go"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/oracle"
	"github.com/transferia/transferia/pkg/providers/oracle/common"
	"github.com/transferia/transferia/pkg/providers/oracle/oraclerecipe"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
)

//go:embed dump/init.sql
var initSQL string

var (
	TransferType = abstract.TransferTypeSnapshotOnly

	// Source is initialized in init() after the Oracle container with custom TZ is started.
	Source oracle.OracleSource

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

// tzCustomizer adds a TZ env var to the Oracle container so that the database
// timezone differs from UTC, reproducing the production issue where SYSDATE
// values shift by 3 hours (Europe/Moscow UTC+3 -> UTC) during snapshot transfer.
type tzCustomizer struct {
	tz string
}

func (c *tzCustomizer) Customize(req *testcontainers_go.GenericContainerRequest) error {
	if req.Env == nil {
		req.Env = make(map[string]string)
	}
	req.Env["TZ"] = c.tz
	return nil
}

func init() {
	_ = os.Setenv("YC", "1")

	ctx := context.Background()

	// Start the Oracle container with Europe/Moscow timezone BEFORE calling
	// RecipeOracleSource. init() runs after package-level var initializers,
	// so we must NOT call RecipeOracleSource in the var block above.
	if _, ok := os.LookupEnv("RECIPE_ORACLE_HOST"); !ok {
		if _, err := oraclerecipe.Run(ctx, oraclerecipe.DefaultImage, &tzCustomizer{tz: "Europe/Moscow"}); err != nil {
			panic(fmt.Sprintf("start oracle container with TZ=Europe/Moscow: %v", err))
		}
	}
	// Now RecipeOracleSource finds RECIPE_ORACLE_* already set — PrepareContainer is a no-op.
	Source = *oraclerecipe.RecipeOracleSource()

	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)

	// Diagnostic: log Oracle session and database timezone settings.
	diag, err := runOracleDiagnostics(ctx)
	if err != nil {
		panic(fmt.Sprintf("oracle diagnostics: %v", err))
	}
	fmt.Printf("[DIAG] %s\n", diag)
	fmt.Printf("[DIAG] Go time.Local = %s\n", time.Local)

	if err := oraclerecipe.ExecSQL(ctx, &Source, initSQL); err != nil {
		panic(err)
	}
}

// runOracleDiagnostics queries Oracle for timezone-related session state.
func runOracleDiagnostics(ctx context.Context) (string, error) {
	db, err := common.CreateConnection(&Source)
	if err != nil {
		return "", fmt.Errorf("create connection: %w", err)
	}
	defer db.Close()

	row := db.QueryRowContext(ctx,
		"SELECT SESSIONTIMEZONE, DBTIMEZONE, TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS TZR') FROM DUAL")
	var sessionTZ, dbTZ, sysTS string
	if err := row.Scan(&sessionTZ, &dbTZ, &sysTS); err != nil {
		return "", fmt.Errorf("scan diagnostics: %w", err)
	}
	return fmt.Sprintf("SESSIONTIMEZONE=%s DBTIMEZONE=%s SYSTIMESTAMP=%s", sessionTZ, dbTZ, sysTS), nil
}

func TestDateTimestampTZ(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Oracle source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("DateTimestampTZ", DateTimestampTZ)
	})
}

func DateTimestampTZ(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.DATE_TZ_TEST"}

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)

	// Direct PG connection to read and assert transferred values.
	pool, err := provider_postgres.MakeConnPoolFromDst(&Target, logger.Log)
	require.NoError(t, err)
	defer pool.Close()

	// Helper: Oracle NUMBER maps to PG text, so id must be passed as string.
	assertTimestamp := func(id int, col string, expected time.Time) {
		query := fmt.Sprintf("SELECT %s FROM dt_test.date_tz_test WHERE id = $1", col)
		var val time.Time
		err := pool.QueryRow(context.Background(), query, strconv.Itoa(id)).Scan(&val)
		require.NoError(t, err, "scan %s id=%d", col, id)
		require.Equal(t, expected, val.UTC(),
			"%s row %d: expected %v UTC, got %v UTC", col, id, expected, val.UTC())
	}

	// Row 1: fixed values — col_date and col_ts must preserve the original wall clock.
	assertTimestamp(1, "col_date",
		time.Date(2026, 4, 28, 17, 0, 24, 0, time.UTC))
	assertTimestamp(1, "col_ts",
		time.Date(2026, 4, 28, 17, 0, 24, 123456000, time.UTC))

	// col_tstz: SYS_EXTRACT_UTC converts 17:00:24.123456 +03:00 -> 14:00:24.123456 UTC.
	assertTimestamp(1, "col_tstz",
		time.Date(2026, 4, 28, 14, 0, 24, 123456000, time.UTC))

	// col_tsltz: stored as UTC, SYS_EXTRACT_UTC gives UTC wall clock.
	assertTimestamp(1, "col_tsltz",
		time.Date(2026, 4, 28, 14, 0, 24, 123456000, time.UTC))

	// Row 2: epoch boundary.
	assertTimestamp(2, "col_date",
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
	assertTimestamp(2, "col_ts",
		time.Date(1970, 1, 1, 0, 0, 0, 1000, time.UTC))

	// Row 3: all NULL — verify NULL handling.
	assertNull := func(id int, col string) {
		query := fmt.Sprintf("SELECT %s FROM dt_test.date_tz_test WHERE id = $1", col)
		var val *time.Time
		err := pool.QueryRow(context.Background(), query, strconv.Itoa(id)).Scan(&val)
		require.NoError(t, err, "scan %s id=%d", col, id)
		require.Nil(t, val, "%s row %d must be NULL", col, id)
	}
	assertNull(3, "col_date")
	assertNull(3, "col_ts")

	// Row 4: SYSDATE/SYSTIMESTAMP — diagnostic log only (non-deterministic).
	var date4 time.Time
	if err := pool.QueryRow(context.Background(),
		"SELECT col_date FROM dt_test.date_tz_test WHERE id = '4'").Scan(&date4); err == nil {
		t.Logf("[DIAG] row 4 col_date (SYSDATE) = %v", date4.UTC())
	}

	// Row count check covers all 4 rows.
	helpers.CheckRowsCount(t, &Target, "dt_test", "date_tz_test", 4)
}
