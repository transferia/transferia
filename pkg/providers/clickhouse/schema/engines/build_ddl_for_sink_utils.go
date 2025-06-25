package engines

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

func parseMergeTreeFamilyEngine(sql string) (*mergeTreeFamilyEngine, string, error) {
	_, engineStr, found := parser.ExtractNameClusterEngine(sql)
	if !found {
		return nil, "", fmt.Errorf("invalid sql: could not parse")
	}

	return newMergeTreeFamilyEngine(engineStr), engineStr, nil
}

func setReplicatedEngine(sql, baseEngine, db, table string) (string, error) {
	if isReplicatedEngineType(baseEngine) {
		return sql, nil
	}

	currEngine, engineStr, err := parseMergeTreeFamilyEngine(sql)
	if err != nil {
		return "", xerrors.Errorf("unable to parse engine from ddl: %w", err)
	}
	if engineType(baseEngine) != currEngine.Type {
		return "", xerrors.Errorf("parsed engine(%v) is not equal with passed engine(%v)", currEngine.Type, baseEngine)
	}

	replicatedEngine := newReplicatedEngine(currEngine, db, table)
	return strings.Replace(sql, engineStr, replicatedEngine.String(), 1), nil
}

func setIfNotExists(sql string) string {
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

func makeDistributedDDL(sql, cluster string) string {
	if IsDistributedDDL(sql) {
		return ReplaceCluster(sql, cluster)
	}

	return strings.Replace(sql, "(", fmt.Sprintf(" ON CLUSTER `%v` (", cluster), 1)
}

func setTargetDatabase(ddl string, sourceDB, targetDB string) string {
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

func setAltName(ddl string, targetDB string, names map[string]string) string {
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
