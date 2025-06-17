//go:build !disable_clickhouse_provider

package schema

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	parser "github.com/transferia/transferia/pkg/providers/clickhouse/schema/ddl_parser"
)

func IsDistributedDDL(sql string) bool {
	onCluster, _, found := parser.ExtractNameClusterEngine(sql)
	if !found {
		return false
	}

	return strings.Trim(onCluster, " \t\n\r") != ""
}

func ReplaceCluster(sql, cluster string) string {
	onCluster, _, found := parser.ExtractNameClusterEngine(sql)
	if !found {
		return sql
	}
	if strings.Count(sql, onCluster) > 1 {
		// something went wrong
		return sql
	}

	return strings.Replace(sql, onCluster, fmt.Sprintf(" ON CLUSTER `%s`", cluster), 1)
}

func SetReplicatedEngine(sql, baseEngine, db, table string) (string, error) {
	if IsReplicatedEngineType(baseEngine) {
		return sql, nil
	}

	engine, engineStr, err := ParseMergeTreeFamilyEngine(sql)
	if err != nil {
		return "", xerrors.Errorf("unable to parse engine from ddl: %w", err)
	}
	if EngineType(baseEngine) != engine.Type {
		return "", xerrors.Errorf("parsed engine(%v) is not equal with passed engine(%v)", engine.Type, baseEngine)
	}

	replicatedEngine, err := NewReplicatedEngine(engine, db, table)
	if err != nil {
		return "", xerrors.Errorf("unable to make replicated engine: %w", err)
	}
	return strings.Replace(sql, engineStr, replicatedEngine.String(), 1), nil
}

func SetIfNotExists(sql string) string {
	if !strings.Contains(sql, "IF NOT EXISTS") {
		switch {
		case strings.Contains(sql, "CREATE TABLE"):
			sql = strings.Replace(sql, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
		case strings.Contains(sql, "CREATE MATERIALIZED VIEW"):
			sql = strings.Replace(sql, "CREATE MATERIALIZED VIEW", "CREATE MATERIALIZED VIEW IF NOT EXISTS", 1)
		}
	}
	return sql
}

func MakeDistributedDDL(sql, cluster string) string {
	if IsDistributedDDL(sql) {
		return ReplaceCluster(sql, cluster)
	}

	return strings.Replace(sql, "(", fmt.Sprintf(" ON CLUSTER `%v` (", cluster), 1)
}

func SetTargetDatabase(ddl string, sourceDB, targetDB string) string {
	if targetDB == "" {
		return ddl
	}
	switch {
	case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE %v.", sourceDB)):
		ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE %v.", sourceDB), fmt.Sprintf("CREATE TABLE `%v`.", targetDB), 1)
	case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE `%v`.", sourceDB)):
		ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE `%v`.", sourceDB), fmt.Sprintf("CREATE TABLE `%v`.", targetDB), 1)
	}
	return ddl
}

func SetAltName(ddl string, targetDB string, names map[string]string) string {
	for from, to := range names {
		switch {
		case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE %v.%v", targetDB, from)):
			ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE %v.%v", targetDB, from), fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, to), 1)
		case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE `%v`.%v", targetDB, from)):
			ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE `%v`.%v", targetDB, from), fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, to), 1)
		case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE %v.`%v`", targetDB, from)):
			ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE %v.`%v`", targetDB, from), fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, to), 1)
		case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, from)):
			ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, from), fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, to), 1)
		}
	}
	return ddl
}
