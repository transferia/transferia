package util

import (
	"net"

	"github.com/transferria/transferria/library/go/core/xerrors"
)

// GetFreePort returns free port in operating system that
// you can use for deploying your service
func GetFreePort() (port int, resultError error) {
	lsnr, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer func(lsnr net.Listener) {
		if err := lsnr.Close(); err != nil {
			resultError = NewErrs(err, resultError)
		}
	}(lsnr)
	addr := lsnr.Addr()
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return 0, xerrors.Errorf("wrong type of address: expected '%T', actual '%T'", new(net.TCPAddr), addr)
	}
	return tcpAddr.Port, nil
}
