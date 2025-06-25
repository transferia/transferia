package engines

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

func BuildDDLForSink(
	lgr log.Logger,
	isDstDistributed bool,
	clusterName string,
	database string,
	altNames map[string]string,
	inSqlDDL string,
	isChCreateTableDistributed bool,
	tableID abstract.TableID,
	inEngine string,
) (string, error) {
	sqlDDL := inSqlDDL
	sqlDDL = setTargetDatabase(sqlDDL, tableID.Namespace, database)
	sqlDDL = setAltName(sqlDDL, database, altNames)
	sqlDDL = setIfNotExists(sqlDDL)
	if isChCreateTableDistributed {
		sqlDDL = ReplaceCluster(sqlDDL, clusterName)
	}
	if isDstDistributed {
		if !isChCreateTableDistributed {
			sqlDDL = makeDistributedDDL(sqlDDL, clusterName)
		}
		currEngine := inEngine
		if currEngine == "MaterializedView" {
			underlying := newMergeTreeFamilyEngine(sqlDDL)
			currEngine = string(underlying.Type)
		}

		if isSharedEngineType(currEngine) {
			replicated, err := getReplicatedFromSharedEngineType(currEngine)
			if err != nil {
				return "", xerrors.Errorf("unable to get replicated from shared engine: %w", err)
			}

			sqlDDL = strings.Replace(sqlDDL, currEngine, replicated, 1)
		}

		if isMergeTreeFamily(currEngine) && !isReplicatedEngineType(currEngine) && !isSharedEngineType(currEngine) {
			if query, err := setReplicatedEngine(sqlDDL, currEngine, tableID.Namespace, tableID.Name); err != nil {
				return query, xerrors.Errorf("unable to set replicated table engine: %w", err)
			} else {
				sqlDDL = query
			}
		}
	} // maybe we also should decrease engine
	return sqlDDL, nil
}
