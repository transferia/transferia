package util

import "github.com/transferia/transferia/library/go/core/xerrors"

func GetStringValue[K ~string, Q ~string, V any](m map[K]V, key Q) (string, error) {
	rawValue, err := getRawValue(m, key)
	if err != nil {
		return "", xerrors.Errorf("cannot get raw value by key %q: %w", string(key), err)
	}
	value, ok := any(rawValue).(string)
	if !ok {
		return "", xerrors.Errorf("cannot cast raw value of type %T to string", rawValue)
	}
	return value, nil
}

func GetIntValue[K ~string, Q ~string, V any](m map[K]V, key Q) (int, error) {
	rawValue, err := getRawValue(m, key)
	if err != nil {
		return 0, xerrors.Errorf("cannot get raw value by key %q: %w", string(key), err)
	}
	value, ok := any(rawValue).(int)
	if !ok {
		return 0, xerrors.Errorf("cannot cast raw value of type %T to int", rawValue)
	}
	return value, nil
}

func GetBoolValue[K ~string, Q ~string, V any](m map[K]V, key Q) (bool, error) {
	rawValue, err := getRawValue(m, key)
	if err != nil {
		return false, xerrors.Errorf("cannot get raw value by key %q: %w", string(key), err)
	}
	value, ok := any(rawValue).(bool)
	if !ok {
		return false, xerrors.Errorf("cannot cast raw value of type %T to bool", rawValue)
	}
	return value, nil
}

func getRawValue[K ~string, Q ~string, V any](m map[K]V, key Q) (V, error) {
	value, ok := m[K(key)]
	if !ok {
		var zero V
		return zero, xerrors.Errorf("key %q does not exist", string(key))
	}
	return value, nil
}
