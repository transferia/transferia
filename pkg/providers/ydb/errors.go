package ydb

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	grpc_codes "google.golang.org/grpc/codes"
)

// WrapYDBError marks well-known YDB failures with error codes; already coded and unknown errors are returned unchanged.
func WrapYDBError(err error) error {
	var alreadyCoded coded.CodedError
	if code := ydbErrorCode(err); code != "" && !xerrors.As(err, &alreadyCoded) {
		return coded.New(code, err)
	}
	return err
}

func ydbErrorCode(err error) coded.Code {
	switch {
	case err == nil:
		return ""

	case ydb_go_sdk.IsOperationErrorOverloaded(err):
		return error_codes.YDBOverloaded

	case ydb_go_sdk.IsOperationErrorSchemeError(err),
		ydb_go_sdk.IsOperationErrorNotFoundError(err):
		return error_codes.YDBNotFound

	case ydb_go_sdk.IsOperationError(err, Ydb.StatusIds_UNAUTHORIZED),
		strings.Contains(err.Error(), "no access"):
		return error_codes.YDBAccessDenied

	case ydb_go_sdk.IsOperationErrorUnavailable(err),
		ydb_go_sdk.IsTransportError(err, grpc_codes.Unavailable):
		return error_codes.YDBUnavailable

	case strings.Contains(err.Error(), "cluster discovery failed"),
		strings.Contains(err.Error(), "failed to dial"),
		strings.Contains(err.Error(), "data source name"):
		return error_codes.YDBConnectionFailed
	}
	return ""
}
