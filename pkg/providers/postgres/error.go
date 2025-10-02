package postgres

import (
	"github.com/jackc/pgconn"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
)

// No alias exports here; use codes from codespkg directly

func init() {}

type PgErrorCode string

// PostgreSQL error codes from https://www.postgresql.org/docs/12/errcodes-appendix.html
const (
	ErrcUniqueViolation              PgErrorCode = "23505"
	ErrcWrongObjectType              PgErrorCode = "42809"
	ErrcRelationDoesNotExists        PgErrorCode = "42P01"
	ErrcSchemaDoesNotExists          PgErrorCode = "3F000"
	ErrcInvalidSnapshotIdentifier    PgErrorCode = "22023"
	ErrcObjectNotInPrerequisiteState PgErrorCode = "55000"
	ErrcInvalidPassword              PgErrorCode = "28P01"
	ErrcInvalidAuthSpec              PgErrorCode = "28000"
	ErrcDropTableWithDependencies    PgErrorCode = "2BP01"
	ErrcGeneratedColumnWriteAttempt  PgErrorCode = "42P10"
	ErrcTooManyConnections           PgErrorCode = "53300"
	ErrcUndefinedFunction            PgErrorCode = "42883"
	ErrcAdminShutdown                PgErrorCode = "57P01"
)

func IsPgError(err error, code PgErrorCode) bool {
	var pgErr pgconn.PgError
	pgErrPtr := &pgErr
	if !xerrors.As(err, &pgErrPtr) {
		return false
	}
	return pgErrPtr.Code == string(code)
}

func IsPKeyCheckError(err error) bool {
	var codederr coded.CodedError
	if xerrors.As(err, &codederr) {
		return codederr.Code() == codes.PostgresNoPrimaryKeyCode
	}
	return false
}
