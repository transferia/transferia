package util

import "github.com/transferria/transferria/library/go/core/xerrors"

func Unwrap(err error) error {
	type causer interface {
		Cause() error
	}
	var unErr causer
	if xerrors.As(err, &unErr) {
		return unErr.Cause()
	}
	return nil
}

func WrapErrCh(headerErr error, errCh chan error) error {
	var err error
	for fillErr := range errCh {
		if fillErr != nil {
			if err == nil {
				err = headerErr
			}
			err = xerrors.Errorf("%v, %w", err, fillErr)
		}
	}
	return err
}
