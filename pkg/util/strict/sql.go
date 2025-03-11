package strict

import (
	"database/sql/driver"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// ExpectedSQL automatically extracts values from `database/sql/driver` Valuer.
func ExpectedSQL[ExpectedT any, ResultT any](value any, concreteCast func(any) (ResultT, error)) (any, error) {
	return expectedImpl[ExpectedT](driverValuerGenericCast[ResultT], value, concreteCast)
}

// UnexpectedSQL automatically extracts values from `database/sql/driver` Valuer.
func UnexpectedSQL[ResultT any](value any, concreteCast func(any) (ResultT, error)) (any, error) {
	return unexpectedImpl(driverValuerGenericCast[ResultT], value, concreteCast)
}

func driverValuerGenericCast[ResultT any](value any, concreteCast func(any) (ResultT, error)) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case driver.Valuer:
		vv, err := v.Value()
		if err != nil {
			return nil, xerrors.Errorf("failed to obtain Value: %w", err)
		}
		if vv == nil {
			return nil, nil
		}
		// rather not call self here in case the returned value is a complex type, so that no unexpectedly deep recursion occurs
		vvv, err := concreteCast(vv)
		if err != nil {
			return nil, xerrors.Errorf("failed to cast an obtained Value to the target type: %w", err)
		}
		return vvv, nil
	default:
		vv, err := concreteCast(v)
		if err != nil {
			return nil, xerrors.Errorf("failed to cast to the target type: %w", err)
		}
		return vv, nil
	}
}
