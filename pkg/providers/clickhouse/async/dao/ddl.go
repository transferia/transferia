package dao

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	ch_db_model "github.com/transferia/transferia/pkg/providers/clickhouse/async/model/db"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema/engines"
	"go.ytsaurus.tech/library/go/core/log"
)

type DDLClient interface {
	ch_db_model.Client
	ch_db_model.DDLExecutor
}

type DDLDAO struct {
	db                   DDLClient
	lgr                  log.Logger
	isReplicatedDatabase bool
}

func (d *DDLDAO) DropTable(db, table string) error {
	d.lgr.Infof("Dropping table %s.%s", db, table)
	return d.db.ExecDDL(func(distributed bool, cluster string) (string, error) {
		if distributed {
			return fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER %s", db, table, cluster), nil
		}
		return fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", db, table), nil
	})
}

func (d *DDLDAO) TruncateTable(db, table string) error {
	d.lgr.Infof("Truncating table %s.%s", db, table)
	return d.db.ExecDDL(func(distributed bool, cluster string) (string, error) {
		if distributed {
			return fmt.Sprintf("TRUNCATE TABLE IF EXISTS `%s`.`%s` ON CLUSTER %s", db, table, cluster), nil
		}
		return fmt.Sprintf("TRUNCATE TABLE IF EXISTS `%s`.`%s`", db, table), nil
	})
}

func (d *DDLDAO) TableExists(db, table string) (bool, error) {
	var exists int
	d.lgr.Infof("Checking if table %s.%s exists", db, table)
	err := d.db.QueryRowContext(context.Background(),
		"SELECT 1 FROM `system`.`tables` WHERE `database` = ? and `name` = ?", db, table).Scan(&exists)
	if err != nil {
		if xerrors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, xerrors.Errorf("error checking table exist: %w", err)
	}
	return exists == 1, nil
}

func (d *DDLDAO) CreateTable(db, table string, schema []abstract.ColSchema) error {
	d.lgr.Infof("Creating table %s.%s", db, table)
	return d.db.ExecDDL(func(distributed bool, cluster string) (string, error) {
		var q strings.Builder
		q.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ", db, table))
		if distributed {
			q.WriteString(fmt.Sprintf(" ON CLUSTER %s ", cluster))
		}
		q.WriteString(" ( ")
		cols := yslices.Map(schema, func(col abstract.ColSchema) string {
			return fmt.Sprintf("`%s` %s", col.ColumnName, columntypes.ToChType(col.DataType))
		})
		q.WriteString(strings.Join(cols, ", "))
		q.WriteString(" ) ")

		var engineStr = "MergeTree()"
		if distributed {
			if d.isReplicatedDatabase {
				engineStr = "ReplicatedMergeTree()"
			} else {
				engineStr = fmt.Sprintf("ReplicatedMergeTree(%s, '{replica}')", d.zkPath(db, table))
			}
		}
		q.WriteString(fmt.Sprintf(" ENGINE = %s ", engineStr))

		var keys []string
		for _, col := range schema {
			if col.IsKey() {
				keys = append(keys, col.ColumnName)
			}
		}
		if len(keys) > 0 {
			q.WriteString(fmt.Sprintf(" ORDER BY (%s) ", strings.Join(keys, ", ")))
		} else {
			q.WriteString(" ORDER BY tuple() ")
		}
		return q.String(), nil
	})
}

// DDLDAO is universal mechanism, which can be used over cluster/shard/host
//
// Let's me remind you - 'pkg/clickhouse/async' mechanism works with temporary tables in clickhouse.
// So, we have created dst table, and should create new tmp parts and merge them into dst table.
//
// So, method 'CreateTableAs':
//     * takes engine (with parameters) from dst_table (which is called here 'base')
//     * takes flag 'distributed', which is filled by some another component
//     * creates
//
// glossary:
//     * baseDB      - database of 'dst' table
//     * baseTable   - tableName of 'dst' table
//     * targetDB    - database of 'tmp' table
//     * targetTable - tableName of 'tmp' table

func (d *DDLDAO) CreateTableAs(baseDB, baseTable, targetDB, targetTable string) error {
	var baseEngine string
	if err := d.db.QueryRowContext(
		context.Background(),
		`SELECT engine_full FROM system.tables WHERE database = ? and name = ?`,
		baseDB, baseTable,
	).Scan(&baseEngine); err != nil {
		return xerrors.Errorf("error getting base table engine: %w", err)
	}

	return d.db.ExecDDL(func(distributed bool, cluster string) (string, error) {
		engineStr, err := d.inferEngine(baseEngine, distributed, d.isReplicatedDatabase, targetDB, targetTable)
		if err != nil {
			return "", xerrors.Errorf("error inferring base engine %s: %w", baseEngine, err)
		}

		d.lgr.Infof("Creating table %s.%s as %s.%s with engine %s", targetDB, targetTable, baseDB, baseTable, engineStr)

		result := ""
		if distributed {
			result = fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS "%s"."%s" ON CLUSTER %s AS "%s"."%s"
				ENGINE = %s`,
				targetDB, targetTable, cluster, baseDB, baseTable, engineStr)
		} else {
			result = fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS "%s"."%s" AS "%s"."%s"
			ENGINE = %s`, targetDB, targetTable, baseDB, baseTable, engineStr)
		}

		d.lgr.Infof("build query: %s", result)

		return result, nil
	})
}

func (d *DDLDAO) inferEngine(rawEngine string, needReplicated bool, isReplicatedDatabase bool, db, table string) (string, error) {
	return engines.FixEngine(rawEngine, needReplicated, isReplicatedDatabase, db, table, d.zkPath(db, table))
}

func (d *DDLDAO) zkPath(db, table string) string {
	return fmt.Sprintf("'/clickhouse/tables/{shard}/%s.%s_cdc'", db, table)
}

func resolveIsReplicatedDatabase(db DDLClient, database string) (bool, error) {
	query := "SELECT engine FROM `system`.`databases` WHERE name = ?"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var engine string
	if err := db.QueryRowContext(ctx, query, database).Scan(&engine); err != nil {
		if xerrors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, xerrors.Errorf("error getting database engine: %w", err)
	}
	return engines.IsReplicatedDatabaseEngine(engine), nil
}

// NewDDLDAO binds the DAO to a single database (the one CreateTable/CreateTableAs operate on).
func NewDDLDAO(client DDLClient, lgr log.Logger, database string) (*DDLDAO, error) {
	isReplicatedDatabase, err := resolveIsReplicatedDatabase(client, database)
	if err != nil {
		return nil, xerrors.Errorf("error resolving database engine for %s: %w", database, err)
	}
	return &DDLDAO{db: client, lgr: lgr, isReplicatedDatabase: isReplicatedDatabase}, nil
}
