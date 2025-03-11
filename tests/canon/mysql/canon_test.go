package mysql

import (
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/mysql"
	"github.com/transferria/transferria/tests/canon/validator"
	"github.com/transferria/transferria/tests/helpers"
)

func execBatch(t *testing.T, conn *sql.DB, sqlCommands string) {
	arr := strings.Split(sqlCommands, ";")
	for _, command := range arr {
		if command == "" || command == "\n" {
			continue
		}
		_, err := conn.Exec(command)
		require.NoError(t, err)
	}
}

func TestCanonSource(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga
	Source := &mysql.MysqlSource{
		ClusterID:           os.Getenv("CLUSTER_ID"),
		Host:                os.Getenv("RECIPE_MYSQL_HOST"),
		User:                os.Getenv("RECIPE_MYSQL_USER"),
		Password:            model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database:            os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:                helpers.GetIntFromEnv("RECIPE_MYSQL_PORT"),
		AllowDecimalAsFloat: true,
	}
	Source.WithDefaults()
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
		))
	}()

	tableCase := func(tableName string) func(t *testing.T) {
		return func(t *testing.T) {
			connParams, err := mysql.NewConnectionParams(Source.ToStorageParams())
			require.NoError(t, err)
			conn, err := mysql.Connect(connParams, nil)
			require.NoError(t, err)

			_, err = conn.Exec(fmt.Sprintf(`drop table %s`, tableName))
			require.NoError(t, err)
			execBatch(t, conn, TableSQLs[tableName])

			counterStorage, counterSinkFactory := validator.NewCounter()
			transfer := helpers.MakeTransfer(
				tableName,
				Source,
				&model.MockDestination{
					SinkerFactory: validator.New(
						model.IsStrictSource(Source),
						validator.InitDone(t),
						validator.Canonizator(t),
						validator.TypesystemChecker(mysql.ProviderType, func(colSchema abstract.ColSchema) string {
							return mysql.ClearOriginalType(colSchema)
						},
						),
						counterSinkFactory,
					),
					Cleanup: model.DisabledCleanup,
				},
				abstract.TransferTypeSnapshotAndIncrement,
			)
			transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{Source.Database + "." + tableName}}
			worker := helpers.Activate(t, transfer)

			_, err = conn.Exec(fmt.Sprintf(`truncate table %s`, tableName))
			require.NoError(t, err)
			counterStorage.Truncate(abstract.TableID{
				Namespace: Source.Database,
				Name:      tableName,
			})
			execBatch(t, conn, TableSQLs[tableName])

			srcStorage, err := mysql.NewStorage(Source.ToStorageParams())
			require.NoError(t, err)

			require.NoError(t, helpers.WaitEqualRowsCount(t, Source.Database, tableName, srcStorage, counterStorage, time.Second*60))

			defer worker.Close(t)
		}
	}
	t.Run("initial", tableCase("initial"))
	t.Run("date_types", tableCase("date_types"))
	t.Run("json_types", tableCase("json_types"))
	t.Run("numeric_types_bit", tableCase("numeric_types_bit"))
	t.Run("numeric_types_boolean", tableCase("numeric_types_boolean"))
	t.Run("numeric_types_decimal", tableCase("numeric_types_decimal"))
	t.Run("numeric_types_float", tableCase("numeric_types_float"))
	t.Run("numeric_types_int", tableCase("numeric_types_int"))
	t.Run("string_types", tableCase("string_types"))
	t.Run("string_types_emoji", tableCase("string_types_emoji"))
	// t.Run("spatial_types", tableCase("spatial_types"))
}
