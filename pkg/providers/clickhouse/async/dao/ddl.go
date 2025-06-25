package dao

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/async/model/db"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema/engines"
	"go.ytsaurus.tech/library/go/core/log"
)

type DDLClient interface {
	db.Client
	db.DDLExecutor
}

type DDLDAO struct {
	db  DDLClient
	lgr log.Logger
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
			engineStr = fmt.Sprintf("ReplicatedMergeTree(%s, '{replica}')", d.zkPath(db, table))
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
		engineStr, err := d.inferEngine(baseEngine, distributed, targetDB, targetTable)
		if err != nil {
			return "", xerrors.Errorf("error inferring base engine %s: %w", baseEngine, err)
		}

		d.lgr.Infof("Creating table %s.%s as %s.%s with engine %s", targetDB, targetTable, baseDB, baseTable, engineStr)

		if distributed {
			return fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS "%s"."%s" ON CLUSTER %s AS "%s"."%s"
				ENGINE = %s`,
				targetDB, targetTable, cluster, baseDB, baseTable, engineStr), nil
		}
		return fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS "%s"."%s" AS "%s"."%s"
			ENGINE = %s`, targetDB, targetTable, baseDB, baseTable, engineStr), nil
	})
}

func (d *DDLDAO) inferEngine(rawEngine string, needReplicated bool, db, table string) (string, error) {
	return engines.InferEngineForDAO(rawEngine, needReplicated, db, table, d.zkPath(db, table))
}

func (d *DDLDAO) zkPath(db, table string) string {
	return fmt.Sprintf("'/clickhouse/tables/{shard}/%s.%s_cdc'", db, table)
}

func NewDDLDAO(client DDLClient, lgr log.Logger) *DDLDAO {
	return &DDLDAO{db: client, lgr: lgr}
}
