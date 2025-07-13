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
