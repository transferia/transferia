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
	if e.replicatingParams == nil {
		return fmt.Sprintf("%s()", e.replicatedEngineType)
	} else if len(e.baseEngine.params) == 0 {
		return fmt.Sprintf("%v(%v, %v)", e.replicatedEngineType, e.replicatingParams.ZkPath, e.replicatingParams.Replica)
	} else {
		return fmt.Sprintf("%v(%v, %v, %v)", e.replicatedEngineType, e.replicatingParams.ZkPath, e.replicatingParams.Replica, strings.Join(e.baseEngine.params, ", "))
	}
}

func newReplicatedEngine(baseEngine *anyEngine, db, table string) *replicatedEngine { // makes replicated version of any engine
	replicatedType, _ := getReplicatedEngineType(string(baseEngine.EngineType()))

	var replicatingParams *replicatedEngineParams = nil
	if baseEngine.EngineType() != mergeTree { // TM-8875 - for 'MergeTree' we should't set zookeeper path explicitly
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
