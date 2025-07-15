package engines

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	parser "github.com/transferia/transferia/pkg/providers/clickhouse/schema/ddl_parser"
)

func BuildDDLForHomoSink(
	inSqlDDL string,
	isDstDistributed bool,
	clusterName string,
	targetDatabase string,
	altNames map[string]string,
	sourceTableID abstract.TableID,
) (string, error) {
	sqlDDL := inSqlDDL
	sqlDDL = setTargetDatabase(sqlDDL, sourceTableID.Namespace, targetDatabase)
	sqlDDL = setAltName(sqlDDL, targetDatabase, altNames)
	sqlDDL = setIfNotExists(sqlDDL)
	isDistributedInSqlDDL := isDistributedDDL(inSqlDDL)
	if isDistributedInSqlDDL {
		sqlDDL = replaceCluster(sqlDDL, clusterName)
	}

	currEngineStr, currEngineName, _, _ := parser.ExtractEngineStrEngineParams(sqlDDL)
	currEngineObject := newAnyEngine(currEngineStr)

	if isDstDistributed {
		// add 'ON CLUSTER'
		if !isDistributedInSqlDDL {
			sqlDDL = makeOnClusterClusterName(sqlDDL, clusterName)
		}

		// handle ENGINE
		if currEngineName == "MaterializedView" {
			currEngineName = string(currEngineObject.EngineType())
		}
		if isSharedEngineType(currEngineName) { // 'Shared*' -> 'Replicated*'
			replicated, err := getReplicatedFromSharedEngineType(currEngineName)
			if err != nil {
				return "", xerrors.Errorf("unable to get replicated from shared engine: %w", err)
			}
			sqlDDL = strings.Replace(sqlDDL, currEngineName, replicated, 1)
		}
		if isMergeTreeFamily(currEngineName) && !isReplicatedEngineType(currEngineName) && !isSharedEngineType(currEngineName) {
			if query, err := setReplicatedEngine(sqlDDL, currEngineName, sourceTableID.Namespace, sourceTableID.Name); err != nil {
				return "", xerrors.Errorf("unable to set replicated table engine: %w", err)
			} else {
				sqlDDL = query
			}
		}
		return sqlDDL, nil
	} else {
		if currEngineObject.IsReplicated() {
			// REPLICATED->NOT_REPLICATED - trim replicated parameters
			currEngineObject.UpdateToNotReplicatedEngine()
			return strings.Replace(sqlDDL, currEngineStr, currEngineObject.String(), 1), nil
		} else {
			// NOT_REPLICATED->NOT_REPLICATED - leave engine as is
			return sqlDDL, nil
		}
	}
}
