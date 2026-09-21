package pgerrors

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
)

// Wrap marks well-known PostgreSQL failures with error codes; already coded and unknown errors are returned unchanged.
func Wrap(err error) error {
	var alreadyCoded coded.CodedError
	if code := errorCode(err); code != "" && !xerrors.As(err, &alreadyCoded) {
		return coded.New(code, err)
	}
	return err
}

func errorCode(err error) coded.Code {
	switch {
	case err == nil:
		return ""
	case IsPgError(err, ErrcTooManyConnections):
		return error_codes.PostgresTooManyConnections
	case IsPgError(err, ErrcLockNotAvailable):
		return error_codes.PostgresLockTimeout
	case IsPgError(err, ErrcInvalidCatalogName):
		return error_codes.PostgresDatabaseNotFound
	case IsPgError(err, ErrcInvalidPassword), IsPgError(err, ErrcInvalidAuthSpec):
		return error_codes.InvalidCredential
	}
	return ""
}
