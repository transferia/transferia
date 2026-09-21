package errors

import (
	"net"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
)

const authenticationFailedCode = 516

// WrapConnectError marks connection-stage failures with error codes; already coded and unknown errors are returned unchanged.
func WrapConnectError(err error) error {
	var alreadyCoded coded.CodedError
	if code := connectErrorCode(err); code != "" && !xerrors.As(err, &alreadyCoded) {
		return coded.New(code, err)
	}
	return err
}

func connectErrorCode(err error) coded.Code {
	var opErr *net.OpError
	var dnsErr *net.DNSError
	var exception *clickhouse_go.Exception
	switch {
	case xerrors.As(err, &opErr), xerrors.As(err, &dnsErr):
		return error_codes.Dial
	case xerrors.As(err, &exception) && exception.Code == authenticationFailedCode:
		return error_codes.InvalidCredential
	}
	return ""
}
