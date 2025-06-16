package clickhouse

import "fmt"

type Host struct {
	Name       string
	NativePort int
	HTTPPort   int
	ShardName  string
}

func (h *Host) String() string {
	if h == nil {
		return ""
	}
	return fmt.Sprintf("%v:[%v|%v]", h.Name, h.NativePort, h.HTTPPort)
}

func (h *Host) HostName() string {
	if h == nil {
		return ""
	}
	return h.Name
}
