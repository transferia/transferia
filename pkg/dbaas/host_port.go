package dbaas

import (
	"net"
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// ResolveHostPortWithOverride - function which determines real port
// If port present in 'host' string - it overrides 'port' from config
// At least it's very useful for testing - you can to upraise locally pgHA installation
// And it also can be useful for any user, who has postgres servers on >1 port
// It can be used with any HA installation - for now it's used only with pg
func ResolveHostPortWithOverride(host string, port uint16) (string, uint16, error) {
	if host == "" {
		return host, port, nil
	}

	numColons := strings.Count(host, ":")
	if numColons == 0 {
		// ipv4/host without port
		return host, port, nil
	}
	if numColons > 1 && host[0] != '[' {
		// ipv6 without port
		return host, port, nil
	}

	currHost, currPort, err := net.SplitHostPort(host)
	if err != nil {
		return "", 0, xerrors.Errorf("unable to split to host/port, str: %s, err: %w", host, err)
	}
	if currPort == "" {
		return currHost, port, nil
	}
	currPortNum, err := strconv.Atoi(currPort)
	if err != nil {
		return "", 0, xerrors.Errorf("unable to parse port, portNum: %s", currPort)
	}
	return currHost, uint16(currPortNum), nil
}
