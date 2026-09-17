package gpfdist

import (
	"net"
	"slices"
	"sort"
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

// getLocalIP returns a non-loopback unicast IPv4 address of this host. It is needed when GP cluster runs on the same
// VM as gpfdist (tests/debugging only): GP runs in Docker and must be able to connect to gpfdist listening on the host.
// Interfaces are examined in the order of interfacePriority: on IPv6-only hosts the Docker bridge gateway address
// (e.g. 172.17.0.1 on docker0) is the only IPv4 address available, and it is always reachable from containers.
func getLocalIP() (net.IP, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, xerrors.Errorf("unable to get net interfaces: %w", err)
	}
	sort.SliceStable(interfaces, func(i, j int) bool {
		return interfacePriority(interfaces[i].Name) < interfacePriority(interfaces[j].Name)
	})
	names := make([]string, 0, len(interfaces))
	for _, iface := range interfaces {
		names = append(names, iface.Name)
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			logger.Log.Warn("Unable to get interface addresses", log.String("interface", iface.Name), log.Error(err))
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
			ip = ip.To4()
			if ip == nil || ip.IsLoopback() || !ip.IsGlobalUnicast() {
				continue // Skip IPv6, loopback and link-local addresses.
			}
			logger.Log.Infof("Using IPv4 address %s of interface %s as gpfdist host", ip.String(), iface.Name)
			return ip, nil
		}
	}
	return nil, xerrors.Errorf("no non-loopback, unicast IPv4 address found on interfaces %v", names)
}

// interfacePriority defines the order in which interfaces are examined by getLocalIP:
// host interfaces first (eth* and veth0, the same as in getEth0Addrs), then Docker bridges, then everything else.
func interfacePriority(name string) int {
	switch {
	case strings.HasPrefix(name, "eth"), name == "veth0":
		return 0
	case strings.HasPrefix(name, "docker"), strings.HasPrefix(name, "br-"):
		return 1
	default:
		return 2
	}
}
