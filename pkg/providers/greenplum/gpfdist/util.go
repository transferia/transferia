package gpfdist

import (
	"net"
	"slices"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

func getEth0Addrs() ([]net.Addr, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, xerrors.Errorf("unable to get net interfaces: %w", err)
	}
	eth0Idx := slices.IndexFunc(interfaces, func(i net.Interface) bool { return i.Name == "eth0" })
	if eth0Idx < 0 {
		names := make([]string, len(interfaces))
		for i, iface := range interfaces {
			names[i] = iface.Name
		}
		return nil, xerrors.Errorf("unable to find eth0 in %v", names)
	}
	return interfaces[eth0Idx].Addrs()
}

// replaceWithV6IfEth0 check that provided IP is from eth0 and returns corresponding IPv6.
// If provided IP is not eth0 – returns it without changes.
func replaceWithV6IfEth0(ip net.IP) (net.IP, error) {
	addrs, err := getEth0Addrs()
	if err != nil {
		logger.Log.Warn("Unable to get eth0 addresses", log.Error(err))
		return ip, nil
	}
	found := false
	var ipv6 net.IP
	for _, addr := range addrs {
		var addrIP net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			addrIP = v.IP
		case *net.IPAddr:
			addrIP = v.IP
		}
		if addrIP.Equal(ip) {
			found = true
		}
		if addrIP != nil && addrIP.To4() == nil && !addrIP.IsLoopback() && addrIP.IsGlobalUnicast() {
			ipv6 = addrIP // Skip IPv4, loopback and link-local addresses.
		}
	}
	if !found {
		return ip, nil
	}
	if ipv6 == nil {
		return nil, xerrors.Errorf("IPv6 address not found in %v", addrs)
	}
	return ipv6, nil
}

func LocalAddrFromStorage(gpAddr string) (net.IP, error) {
	conn, err := net.Dial("tcp", gpAddr)
	if err != nil {
		return nil, xerrors.Errorf("unable to dial GP address %s: %w", gpAddr, err)
	}
	defer conn.Close()

	addr := conn.LocalAddr()
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return nil, xerrors.Errorf("expected LocalAddr to be *net.TCPAddr, got %T", addr)
	}
	logger.Log.Infof("Transfer VM's address resolved (%s)", tcpAddr.String())

	return replaceWithV6IfEth0(tcpAddr.IP)
}
