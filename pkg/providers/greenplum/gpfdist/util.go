package gpfdist

import (
	"net"
	"slices"
	"strings"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/config/env"
	"go.ytsaurus.tech/library/go/core/log"
)

func getEth0Addrs() ([]net.Addr, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, xerrors.Errorf("unable to get net interfaces: %w", err)
	}
	eth0Idx := slices.IndexFunc(interfaces, func(i net.Interface) bool { return i.Name == "eth0" || i.Name == "veth0" })
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

	if env.IsTest() && tcpAddr.IP.IsLoopback() {
		logger.Log.Warnf("Dial local address is loopback (%s), resolving from interfaces", tcpAddr.IP.String())
		ip, err := getLocalIP()
		if err != nil {
			return nil, xerrors.Errorf("unable to get local IP: %w", err)
		}
		return ip, nil
	}

	return replaceWithV6IfEth0(tcpAddr.IP)
}

// getLocalIP needed when GP cluster runs on same VM as gpfdist (should be in tests/debugging only).
func getLocalIP() (net.IP, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, xerrors.Errorf("unable to get net interfaces: %w", err)
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 || !strings.HasPrefix(iface.Name, "eth") {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() || !ip.To4().IsGlobalUnicast() {
				continue // Skip IPv6, loopback and link-local addresses.
			}
			return ip, nil
		}
	}
	return nil, xerrors.Errorf("no non-loopback, unicast IPv4 address found")
}
