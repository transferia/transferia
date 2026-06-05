package engines

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// FixEngine
//
// * rawEngine - input string, engine part of DDL CREATE TABLE, in format `engineName()` with optional parameters into brackets
// * isDstReplicated - flag, shows should 'rawEngine' be converted into 'replicated' or 'not-replicated' version of engine
// * isReplicatedDatabase - when true, omit explicit zookeeper_path/replica_name (see IsReplicatedDatabaseEngine)
//
// other three parameters should be used, when build zookeeper path

func FixEngine(inEngineStr string, isDstReplicated, isReplicatedDatabase bool, db, table, zkPath string) (string, error) {
	srcEngineObject := newAnyEngine(inEngineStr) // engine, may contain or may not contain params, with 'ORDER BY', 'SETTINGS'

	if srcEngineObject.IsReplicated() && isDstReplicated && len(srcEngineObject.Params()) < 2 {
		return inEngineStr, nil // Already no arguments, nothing to replace or remove.
	}

	if srcEngineObject.IsReplicated() {
		if isDstReplicated {
			// REPLICATED->REPLICATED
			replicatedEngineObj, err := newReplicatedEngineFromReplicated(srcEngineObject)
			if err != nil {
				return "", xerrors.Errorf("unable to make newReplicatedEngineFromReplicated, err: %w", err)
			}
			if isReplicatedDatabase {
				replicatedEngineObj.replicatingParams = nil // drop zookeeper_path/replica_name, keep base params
				return strings.Replace(inEngineStr, srcEngineObject.RawStringEnginePart(), replicatedEngineObj.String(), 1), nil
			}
			// just replace zookeeper path
			result := strings.Replace(inEngineStr, replicatedEngineObj.replicatingParams.ZkPath, zkPath, 1)
			return result, nil
		} else {
			// REPLICATED->NOT_REPLICATED - trim replicated parameters
			dst := srcEngineObject.Clone()
			dst.UpdateToNotReplicatedEngine()
			result := strings.Replace(inEngineStr, srcEngineObject.RawStringEnginePart(), dst.String(), 1)
			return result, nil
		}
	} else {
		if isDstReplicated {
			// NOT_REPLICATED->REPLICATED - build zookeeper path from 'db' & 'table'
			replicatedEngineObj := newReplicatedEngine(srcEngineObject, db, table, isReplicatedDatabase)
			result := strings.Replace(inEngineStr, srcEngineObject.RawStringEnginePart(), replicatedEngineObj.String(), 1)
			return result, nil
		} else {
			// NOT_REPLICATED->NOT_REPLICATED - leave engine as is
			return inEngineStr, nil
		}
	}
}
