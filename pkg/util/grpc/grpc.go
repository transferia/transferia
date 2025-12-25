package grpc

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
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
