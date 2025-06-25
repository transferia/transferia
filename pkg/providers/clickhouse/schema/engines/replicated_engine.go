package engines

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type replicatedEngineParams struct {
	ZooPath   string
	Replica   string
	TableName string
}

type replicatedEngine struct {
	BaseEngine *mergeTreeFamilyEngine
	Type       engineType
	Params     *replicatedEngineParams
}

func (e *replicatedEngine) IsEngine() {}

func (e *replicatedEngine) String() string {
	if len(e.BaseEngine.Params) == 0 {
		return fmt.Sprintf("%v(%v, %v)", e.Type, e.Params.ZooPath, e.Params.Replica)
	}
	return fmt.Sprintf("%v(%v, %v, %v)", e.Type, e.Params.ZooPath, e.Params.Replica, strings.Join(e.BaseEngine.Params, ", "))
}

func newReplicatedEngine(baseEngine *mergeTreeFamilyEngine, db, table string) *replicatedEngine {
	replicatedType, _ := getReplicatedEngineType(string(baseEngine.Type))
	tableName := fmt.Sprintf("%v.%v_cdc", db, table)
	return &replicatedEngine{
		BaseEngine: baseEngine,
		Type:       engineType(replicatedType),
		Params: &replicatedEngineParams{
			ZooPath:   fmt.Sprintf("'/clickhouse/tables/{shard}/%v'", tableName),
			Replica:   "'{replica}'",
			TableName: tableName,
		},
	}
}

//---

func parseReplicatedEngineParams(zkPathArg, replicaArg string) (*replicatedEngineParams, error) {
	zkPathTokens := strings.Split(zkPathArg, "/")
	if len(zkPathTokens) < 2 {
		return nil, xerrors.Errorf("zk path doesn`t seem to be a path: %v", zkPathArg)
	}
	return &replicatedEngineParams{
		ZooPath:   zkPathArg,
		Replica:   replicaArg,
		TableName: zkPathTokens[len(zkPathTokens)-1],
	}, nil
}
