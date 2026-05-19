package pgerrors

import (
	"github.com/jackc/pgconn"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

type PgErrorCode string

// PostgreSQL error codes from https://www.postgresql.org/docs/current/errcodes-appendix.html
const (
	ErrcUniqueViolation              PgErrorCode = "23505"
	ErrcWrongObjectType              PgErrorCode = "42809"
	ErrcRelationDoesNotExists        PgErrorCode = "42P01"
	ErrcSchemaDoesNotExists          PgErrorCode = "3F000"
	ErrcInvalidSnapshotIdentifier    PgErrorCode = "22023"
	ErrcObjectNotInPrerequisiteState PgErrorCode = "55000"
	ErrcInvalidPassword              PgErrorCode = "28P01"
	ErrcInvalidAuthSpec              PgErrorCode = "28000"
	ErrcInvalidCatalogName           PgErrorCode = "3D000"
	ErrcDropTableWithDependencies    PgErrorCode = "2BP01"
	ErrcGeneratedColumnWriteAttempt  PgErrorCode = "42P10"
	ErrcTooManyConnections           PgErrorCode = "53300"
	ErrcUndefinedFunction            PgErrorCode = "42883"
	ErrcAdminShutdown                PgErrorCode = "57P01"
	ErrcInternalError                PgErrorCode = "XX000"
	ErrcUndefinedFile                PgErrorCode = "58P01"
	ErrcProtocolViolation            PgErrorCode = "08P01"
	ErrcDuplicateObject              PgErrorCode = "42710"
	ErrcDuplicateTable               PgErrorCode = "42P07"
	ErrcDuplicateFunction            PgErrorCode = "42723"
	ErrcDuplicateSchema              PgErrorCode = "42P06"
)

func IsPgError(err error, code PgErrorCode) bool {
	var pgErr *pgconn.PgError
	if !xerrors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == string(code)
}

// AnyErrHasCode returns true if at least one error in the slice is a pgconn.PgError
// with a Code matching any of the provided codes.
func AnyErrHasCode(errs []error, codes ...PgErrorCode) bool {
	codeSet := make(map[string]bool, len(codes))
	for _, c := range codes {
		codeSet[string(c)] = true
	}
	for _, err := range errs {
		var pgErr *pgconn.PgError
		if xerrors.As(err, &pgErr) && codeSet[pgErr.Code] {
			return true
		}
	}
	return false
}
