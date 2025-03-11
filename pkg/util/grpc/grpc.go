package grpc

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"google.golang.org/grpc/status"
)

type GRPCStatusError interface {
	error
	GRPCStatus() *status.Status
}

func UnwrapStatusError(err error) (bool, GRPCStatusError) {
	var statusErr GRPCStatusError
	if xerrors.As(err, &statusErr) {
		return true, statusErr
	}
	return false, nil
}
