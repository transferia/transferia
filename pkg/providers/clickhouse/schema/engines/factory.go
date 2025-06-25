package engines

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func newReplicated(baseEngine *mergeTreeFamilyEngine, db, table string) (*replicatedEngine, error) {
	if isReplicatedEngineType(string(baseEngine.Type)) {
		// if it was 'Replicated*'
		return convertToReplicated(baseEngine)
	} else {
		// if we are convering 'engine'->'replicatedEngine'
		return newReplicatedEngine(baseEngine, db, table), nil
	}
}

func convertToReplicated(inEngine *mergeTreeFamilyEngine) (*replicatedEngine, error) {
	baseType, err := getBaseEngineType(string(inEngine.Type))
	if err != nil {
		return nil, xerrors.Errorf("unable to discover base engine type: %w", err)
	}
	if len(inEngine.Params) < 2 {
		return nil, xerrors.Errorf("invalid params - it must be at least 2, but got %v: %v", len(inEngine.Params), inEngine.Params)
	}
	replicatedParams, err := parseReplicatedEngineParams(inEngine.Params[0], inEngine.Params[1])
	if err != nil {
		return nil, xerrors.Errorf("invalid params: %w", err)
	}
	return &replicatedEngine{
		BaseEngine: &mergeTreeFamilyEngine{
			Type:   engineType(baseType),
			Params: inEngine.Params[2:],
		},
		Type:   inEngine.Type,
		Params: replicatedParams,
	}, nil
}
