package engines

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// FixEngine
//
// * rawEngine - input string, engine part of DDL CREATE TABLE, in format `engineName()` with optional parameters into brackets
// * targetReplicated - flag, shows should 'rawEngine' be converted into 'replicated' or 'not-replicated' version of engine
//
// other three parameters should be used, when build zookeeper path

func FixEngine(inEngineStr string, isDstReplicated bool, db, table, zkPath string) (string, error) {
	srcEngineObject := newAnyEngine(inEngineStr) // engine, may contain or may not contain params, with 'ORDER BY', 'SETTINGS'

	if srcEngineObject.IsReplicated() {
		if isDstReplicated {
			// REPLICATED->REPLICATED - just replace zookeeper path
			replicatedEngineObj, err := newReplicatedEngineFromReplicated(srcEngineObject)
			if err != nil {
				return "", xerrors.Errorf("unable to make newReplicatedEngineFromReplicated, err: %w", err)
			}
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
			replicatedEngineObj := newReplicatedEngine(srcEngineObject, db, table)
			result := strings.Replace(inEngineStr, srcEngineObject.RawStringEnginePart(), replicatedEngineObj.String(), 1)
			return result, nil
		} else {
			// NOT_REPLICATED->NOT_REPLICATED - leave engine as is
			return inEngineStr, nil
		}
	}
}
