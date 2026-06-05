package engines

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type replicatedEngine struct {
	baseEngine           *anyEngine // 'base' engine object - not replicated engine - for example: 'MergeTree'
	replicatedEngineType engineType // 'replicated' engine name - for example: 'ReplicatedMergeTree'
	replicatingParams    *replicatedEngineParams
}

func (e *replicatedEngine) String() string {
	// [zookeeper_path, replica_name] (when set explicitly) followed by base engine params (e.g. ReplacingMergeTree's ver).
	var params []string
	if e.replicatingParams != nil {
		params = append(params, e.replicatingParams.ZkPath, e.replicatingParams.Replica)
	}
	params = append(params, e.baseEngine.params...)
	return fmt.Sprintf("%v(%v)", e.replicatedEngineType, strings.Join(params, ", "))
}

// newReplicatedEngine makes a replicated version of any engine.
// omitZkPath skips the explicit zookeeper_path/replica_name (see IsReplicatedDatabaseEngine).
func newReplicatedEngine(baseEngine *anyEngine, db, table string, omitZkPath bool) *replicatedEngine {
	replicatedType, _ := getReplicatedEngineType(string(baseEngine.EngineType()))

	var replicatingParams *replicatedEngineParams = nil
	if baseEngine.EngineType() != mergeTree && !omitZkPath { // TM-8875 - for 'MergeTree' we shouldn't set zookeeper path explicitly.
		tableName := fmt.Sprintf("%v.%v_cdc", db, table)
		replicatingParams, _ = newReplicatedEngineParams(
			fmt.Sprintf("'/clickhouse/tables/{shard}/%v'", tableName),
			"'{replica}'",
		)
	}

	return &replicatedEngine{
		baseEngine:           baseEngine,
		replicatedEngineType: engineType(replicatedType),
		replicatingParams:    replicatingParams,
	}
}

func newReplicatedEngineFromReplicated(inReplicatedEngine *anyEngine) (*replicatedEngine, error) { // for DAO
	baseEngine := inReplicatedEngine.Clone()
	baseEngine.UpdateToNotReplicatedEngine()

	replicatedParams, err := newReplicatedEngineParams(inReplicatedEngine.params[0], inReplicatedEngine.params[1])
	if err != nil {
		return nil, xerrors.Errorf("invalid params: %w", err)
	}
	return &replicatedEngine{
		baseEngine:           baseEngine,
		replicatedEngineType: inReplicatedEngine.EngineType(),
		replicatingParams:    replicatedParams,
	}, nil
}
