package validators

import (
	"net"
	"regexp"
	"strconv"

	"github.com/transferria/transferria/library/go/core/xerrors"
)

const (
	hostnameRegex = "^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])(:?[0-9]*)$"
	maxPortValue  = 65535
)

func HostPort(host string, port string) error {
	if err := Host(host); err != nil {
		//nolint:descriptiveerrors
		return err
	}

	if err := Port(port); err != nil {
		//nolint:descriptiveerrors
		return err
	}
	return nil
}

func Host(host string) error {
	matchedHostname, _ := regexp.Match(hostnameRegex, []byte(host))
	if !matchedHostname {
		// host is ip
		if ip := net.ParseIP(host); ip == nil {
			return xerrors.Errorf("invalid host: %q", host)
		}
		return nil
	}

	return nil
}

func Port(port string) error {
	p, err := strconv.Atoi(port)
	if err != nil {
		return xerrors.Errorf("could not parse port %q to int", port)
	}
	if p > maxPortValue {
		return xerrors.Errorf("invalid port value %q > maxPortValue", port)
	}
	return nil
}
