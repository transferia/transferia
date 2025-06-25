package engines

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// is* utils

func isMergeTreeFamily(inEngine string) bool {
	switch engineType(inEngine) {
	case mergeTree, replacingMergeTree, summingMergeTree, aggregatingMergeTree, collapsingMergeTree, versionedCollapsingMergeTree, graphiteMergeTree, replicatedMergeTree, replicatedReplacingMergeTree, replicatedSummingMergeTree, replicatedAggregatingMergeTree, replicatedCollapsingMergeTree, replicatedVersionedCollapsingMergeTree, replicatedGraphiteMergeTree, sharedMergeTree, sharedReplacingMergeTree, sharedSummingMergeTree, sharedAggregatingMergeTree, sharedCollapsingMergeTree, sharedVersionedCollapsingMergeTree, sharedGraphiteMergeTree:
		return true
	default:
		return false
	}
}

func isReplicatedEngineType(inEngine string) bool {
	switch engineType(inEngine) {
	case replicatedMergeTree, replicatedReplacingMergeTree, replicatedSummingMergeTree, replicatedAggregatingMergeTree, replicatedCollapsingMergeTree, replicatedVersionedCollapsingMergeTree, replicatedGraphiteMergeTree:
		return true
	default:
		return false
	}
}

func isSharedEngineType(inEngine string) bool {
	switch engineType(inEngine) {
	case sharedMergeTree, sharedReplacingMergeTree, sharedSummingMergeTree, sharedAggregatingMergeTree, sharedCollapsingMergeTree, sharedVersionedCollapsingMergeTree, sharedGraphiteMergeTree:
		return true
	default:
		return false
	}
}

// get* utils

func getReplicatedFromSharedEngineType(inEngine string) (string, error) {
	if !isSharedEngineType(inEngine) {
		return "", xerrors.Errorf("engine must be shared family engine: %v", inEngine)
	}

	replicated := strings.Replace(inEngine, "Shared", "Replicated", 1)

	if isReplicatedEngineType(replicated) {
		return replicated, nil
	}
	return "", xerrors.Errorf("engine %v does not have replicated version", inEngine)
}

func getReplicatedEngineType(inEngine string) (string, error) {
	if isReplicatedEngineType(inEngine) {
		return inEngine, nil
	}

	replicated := fmt.Sprintf("Replicated%v", inEngine)
	if isReplicatedEngineType(replicated) {
		return replicated, nil
	}
	return "", xerrors.Errorf("engine %v does not have replicated version", inEngine)
}

func getBaseEngineType(inEngine string) (string, error) {
	if !isMergeTreeFamily(inEngine) {
		return "", xerrors.Errorf("invalid engine: %v", inEngine)
	}

	if isReplicatedEngineType(inEngine) {
		return strings.TrimPrefix(inEngine, "Replicated"), nil
	}

	if isSharedEngineType(inEngine) {
		return strings.TrimPrefix(inEngine, "Shared"), nil
	}

	return inEngine, nil
}

// infer engine

func InferEngineForDAO(rawEngine string, needReplicated bool, db, table, zkPath string) (string, error) {
	engineSpec := rawEngine
	idx := tryFindNextStatement(engineSpec, 0)
	if idx != -1 {
		engineSpec = rawEngine[:idx]
	}
	currEndine := newMergeTreeFamilyEngine(engineSpec) // parsed part of engine, may contain or may not contain params
	engStr := currEndine.String()
	baseIsReplicated := isReplicatedEngineType(string(currEndine.Type))

	if needReplicated {
		replEng, err := newReplicated(currEndine, db, table)
		if err != nil {
			return "", xerrors.Errorf("unable to build replication engine, err: %w", err)
		}
		if baseIsReplicated {
			return strings.Replace(rawEngine, replEng.Params.ZooPath, zkPath, 1), nil
		} else {
			return strings.Replace(rawEngine, engStr, replEng.String(), 1), nil
		}
	}

	if baseIsReplicated {
		currEndine.Type = engineType(strings.Replace(string(currEndine.Type), "Replicated", "", 1))
		currEndine.Params = currEndine.Params[2:]
		return strings.Replace(rawEngine, engStr, currEndine.String(), 1), nil
	}
	return rawEngine, nil
}

func tryFindNextStatement(sql string, from int) int {
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
