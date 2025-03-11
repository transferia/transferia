package strict

import "github.com/transferria/transferria/library/go/core/xerrors"

func expectedImpl[ExpectedT any, ResultT any](genericCast func(value any, concreteCast func(any) (ResultT, error)) (any, error), value any, concreteCast func(any) (ResultT, error)) (any, error) {
	if value == nil {
		return nil, nil
	}

	if vExpected, ok := value.(ExpectedT); ok {
		result, err := genericCast(vExpected, concreteCast)
		if err != nil {
			return nil, xerrors.Errorf("failed to cast %T: %w", vExpected, err)
		}
		return result, nil
	}
	return unexpectedImpl(genericCast, value, concreteCast)
}

func unexpectedImpl[ResultT any](genericCast func(value any, concreteCast func(any) (ResultT, error)) (any, error), value any, concreteCast func(any) (ResultT, error)) (any, error) {
	if value == nil {
		return nil, nil
	}

	result, err := genericCast(value, concreteCast)
	if err != nil {
		return nil, xerrors.Errorf("failed to cast unexpected type %T: %w", value, err)
	}
	return result, nil
}
