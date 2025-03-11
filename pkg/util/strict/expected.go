package strict

import "github.com/transferria/transferria/library/go/core/xerrors"

// Expected is a pure wrapper over concrete cast.
func Expected[ExpectedT any, ResultT any](value any, concreteCast func(any) (ResultT, error)) (any, error) {
	return expectedImpl[ExpectedT](concreteCastAsGenericCast[ResultT], value, concreteCast)
}

// Unexpected is a pure wrapper over concrete cast.
func Unexpected[ResultT any](value any, concreteCast func(any) (ResultT, error)) (any, error) {
	return unexpectedImpl(concreteCastAsGenericCast[ResultT], value, concreteCast)
}

func concreteCastAsGenericCast[ResultT any](value any, concreteCast func(any) (ResultT, error)) (any, error) {
	if value == nil {
		return nil, nil
	}

	result, err := concreteCast(value)
	if err != nil {
		return nil, xerrors.Errorf("failed to cast to the target type: %w", err)
	}
	return result, nil
}
