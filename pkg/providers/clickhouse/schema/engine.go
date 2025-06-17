//go:build !disable_clickhouse_provider

package schema

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	parser "github.com/transferia/transferia/pkg/providers/clickhouse/schema/ddl_parser"
)

type EngineType string

type Engine interface {
	IsEngine()
	String() string
}

type MergeTreeFamilyEngine struct {
	Type   EngineType
	Params []string
}

type ReplicatedEngine struct {
	BaseEngine MergeTreeFamilyEngine
	Type       EngineType
	Params     ReplicatedEngineParams
}

type ReplicatedEngineParams struct {
	ZooPath   string
	Replica   string
	TableName string
}

const (
	MaterializedView             = EngineType("MaterializedView")
	MergeTree                    = EngineType("MergeTree")
	ReplacingMergeTree           = EngineType("ReplacingMergeTree")
	SummingMergeTree             = EngineType("SummingMergeTree")
	AggregatingMergeTree         = EngineType("AggregatingMergeTree")
	CollapsingMergeTree          = EngineType("CollapsingMergeTree")
	VersionedCollapsingMergeTree = EngineType("VersionedCollapsingMergeTree")
	GraphiteMergeTree            = EngineType("GraphiteMergeTree")

	ReplicatedMergeTree                    = EngineType("ReplicatedMergeTree")
	ReplicatedReplacingMergeTree           = EngineType("ReplicatedReplacingMergeTree")
	ReplicatedSummingMergeTree             = EngineType("ReplicatedSummingMergeTree")
	ReplicatedAggregatingMergeTree         = EngineType("ReplicatedAggregatingMergeTree")
	ReplicatedCollapsingMergeTree          = EngineType("ReplicatedCollapsingMergeTree")
	ReplicatedVersionedCollapsingMergeTree = EngineType("ReplicatedVersionedCollapsingMergeTree")
	ReplicatedGraphiteMergeTree            = EngineType("ReplicatedGraphiteMergeTree")

	// SharedMergeTree family is available on ch.inc exclusively
	SharedMergeTree                    = EngineType("SharedMergeTree")
	SharedReplacingMergeTree           = EngineType("SharedReplacingMergeTree")
	SharedSummingMergeTree             = EngineType("SharedSummingMergeTree")
	SharedAggregatingMergeTree         = EngineType("SharedAggregatingMergeTree")
	SharedCollapsingMergeTree          = EngineType("SharedCollapsingMergeTree")
	SharedVersionedCollapsingMergeTree = EngineType("SharedVersionedCollapsingMergeTree")
	SharedGraphiteMergeTree            = EngineType("SharedGraphiteMergeTree")
)

func IsMergeTreeFamily(engine string) bool {
	switch EngineType(engine) {
	case MergeTree, ReplacingMergeTree, SummingMergeTree, AggregatingMergeTree, CollapsingMergeTree, VersionedCollapsingMergeTree, GraphiteMergeTree, ReplicatedMergeTree, ReplicatedReplacingMergeTree, ReplicatedSummingMergeTree, ReplicatedAggregatingMergeTree, ReplicatedCollapsingMergeTree, ReplicatedVersionedCollapsingMergeTree, ReplicatedGraphiteMergeTree, SharedMergeTree, SharedReplacingMergeTree, SharedSummingMergeTree, SharedAggregatingMergeTree, SharedCollapsingMergeTree, SharedVersionedCollapsingMergeTree, SharedGraphiteMergeTree:
		return true
	default:
		return false
	}
}

func IsReplicatedEngineType(engine string) bool {
	switch EngineType(engine) {
	case ReplicatedMergeTree, ReplicatedReplacingMergeTree, ReplicatedSummingMergeTree, ReplicatedAggregatingMergeTree, ReplicatedCollapsingMergeTree, ReplicatedVersionedCollapsingMergeTree, ReplicatedGraphiteMergeTree:
		return true
	default:
		return false
	}
}

func IsSharedEngineType(engine string) bool {
	switch EngineType(engine) {
	case SharedMergeTree, SharedReplacingMergeTree, SharedSummingMergeTree, SharedAggregatingMergeTree, SharedCollapsingMergeTree, SharedVersionedCollapsingMergeTree, SharedGraphiteMergeTree:
		return true
	default:
		return false
	}
}

func GetReplicatedFromSharedEngineType(engine string) (string, error) {
	if !IsSharedEngineType(engine) {
		return "", xerrors.Errorf("engine must be shared family engine: %v", engine)
	}

	replicated := strings.Replace(engine, "Shared", "Replicated", 1)

	if IsReplicatedEngineType(replicated) {
		return replicated, nil
	}
	return "", xerrors.Errorf("engine %v does not have replicated version", engine)
}

func GetReplicatedEngineType(engine string) (string, error) {
	if IsReplicatedEngineType(engine) {
		return engine, nil
	}

	replicated := fmt.Sprintf("Replicated%v", engine)
	if IsReplicatedEngineType(replicated) {
		return replicated, nil
	}
	return "", xerrors.Errorf("Engine %v does not have replicated version", engine)
}

func GetBaseEngineType(engine string) (string, error) {
	if !IsMergeTreeFamily(engine) {
		return "", xerrors.Errorf("invalid engine: %v", engine)
	}

	if IsReplicatedEngineType(engine) {
		return strings.TrimPrefix(engine, "Replicated"), nil
	}

	if IsSharedEngineType(engine) {
		return strings.TrimPrefix(engine, "Shared"), nil
	}

	return engine, nil
}

func (mt *MergeTreeFamilyEngine) IsEngine() {}

func (mt *MergeTreeFamilyEngine) String() string {
	if len(mt.Params) == 0 {
		return string(mt.Type)
	}
	return fmt.Sprintf("%v(%v)", mt.Type, strings.Join(mt.Params, ", "))
}

func GetEngine(engineStrSQL string) (engine *MergeTreeFamilyEngine, engineStr string, err error) {
	engine = new(MergeTreeFamilyEngine)
	engineStr = engineStrSQL

	paramsStart := strings.Index(engineStrSQL, "(")
	paramsEnd := strings.LastIndex(engineStrSQL, ")")
	if paramsStart > 0 && paramsEnd > 0 {
		paramsStr := strings.Trim(engineStrSQL[paramsStart+1:paramsEnd], "\r\t\n ")
		if paramsStr != "" {
			engine.Params = strings.Split(paramsStr, ", ")
		}

		engine.Type = EngineType(engineStrSQL[:paramsStart])
	} else {
		engine.Type = EngineType(strings.Trim(engineStr, "\n\r\t "))
	}

	return engine, engineStr, err
}

func ParseMergeTreeFamilyEngine(sql string) (*MergeTreeFamilyEngine, string, error) {
	_, engineStr, found := parser.ExtractNameClusterEngine(sql)
	if !found {
		return nil, "", fmt.Errorf("invalid sql: could not parse")
	}

	return GetEngine(engineStr)
}

func TryFindNextStatement(sql string, from int) int {
	possibleStatements := []string{
		" ORDER BY",
		" PARTITION BY",
		" PRIMARY KEY",
		" SETTINGS",
	}

	nearestStatement := -1
	diff := len(sql) - from
	for _, stmt := range possibleStatements {
		if idx := strings.Index(sql, stmt); idx != -1 && idx > from {
			stmtDiff := idx - from
			if stmtDiff < diff {
				nearestStatement = idx
				diff = stmtDiff
			}
		}
	}
	return nearestStatement
}

func (re *ReplicatedEngine) IsEngine() {}

func (re *ReplicatedEngine) String() string {
	if len(re.BaseEngine.Params) == 0 {
		return fmt.Sprintf("%v(%v, %v)", re.Type, re.Params.ZooPath, re.Params.Replica)
	}
	return fmt.Sprintf("%v(%v, %v, %v)", re.Type, re.Params.ZooPath, re.Params.Replica, strings.Join(re.BaseEngine.Params, ", "))
}

func NewReplicatedEngine(baseEngine *MergeTreeFamilyEngine, db, table string) (ReplicatedEngine, error) {
	if IsReplicatedEngineType(string(baseEngine.Type)) {
		return ConvertToReplicated(baseEngine)
	}
	return newReplicated(baseEngine, db, table), nil
}

func ConvertToReplicated(engine *MergeTreeFamilyEngine) (ReplicatedEngine, error) {
	baseType, err := GetBaseEngineType(string(engine.Type))
	if err != nil {
		return ReplicatedEngine{}, xerrors.Errorf("unable to discover base engine type: %w", err)
	}
	if len(engine.Params) < 2 {
		return ReplicatedEngine{}, xerrors.Errorf("invalid params - it must be at least 2, but got %v: %v", len(engine.Params), engine.Params)
	}
	replicatedParams, err := ParseReplicatedEngineParams(engine.Params[0], engine.Params[1])
	if err != nil {
		return ReplicatedEngine{}, xerrors.Errorf("invalid params: %w", err)
	}
	return ReplicatedEngine{
		BaseEngine: MergeTreeFamilyEngine{
			Type:   EngineType(baseType),
			Params: engine.Params[2:],
		},
		Type:   engine.Type,
		Params: replicatedParams,
	}, nil
}

func newReplicated(baseEngine *MergeTreeFamilyEngine, db, table string) ReplicatedEngine {
	replicatedType, _ := GetReplicatedEngineType(string(baseEngine.Type))
	tableName := fmt.Sprintf("%v.%v_cdc", db, table)
	return ReplicatedEngine{
		BaseEngine: *baseEngine,
		Type:       EngineType(replicatedType),
		Params: ReplicatedEngineParams{
			ZooPath:   fmt.Sprintf("'/clickhouse/tables/{shard}/%v'", tableName),
			Replica:   "'{replica}'",
			TableName: tableName,
		},
	}
}

func ParseReplicatedEngineParams(zooPathArg, replicaArg string) (ReplicatedEngineParams, error) {
	zooPathTokens := strings.Split(zooPathArg, "/")
	if len(zooPathTokens) < 2 {
		return ReplicatedEngineParams{}, xerrors.Errorf("zoo path doesn`t seem to be a path: %v", zooPathArg)
	}
	return ReplicatedEngineParams{
		ZooPath:   zooPathArg,
		Replica:   replicaArg,
		TableName: zooPathTokens[len(zooPathTokens)-1],
	}, nil
}
