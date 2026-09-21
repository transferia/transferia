package network

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	gopsutil_net "github.com/shirou/gopsutil/v3/net"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/connection"
	"go.ytsaurus.tech/library/go/core/log"
)

type LabeledPort struct {
	Label string
	Port  int
}

func CheckConnections(labeledPorts ...LabeledPort) error {
	pid := os.Getpid()
	visited := map[int]bool{}

	if err := backoff.Retry(func() error {
		connections, err := gopsutil_net.Connections("all")
		if err != nil {
			return xerrors.Errorf("Unable to get connections: %w", err)
		}

		for _, labeledPort := range labeledPorts {
			port, label := labeledPort.Port, labeledPort.Label
			if _, seen := visited[port]; !seen {
				visited[port] = true
				var leaks []string
				for _, conn := range connections {
					if conn.Status == "ESTABLISHED" &&
						int(conn.Pid) == pid &&
						conn.Raddr.Port == uint32(port) {
						leaks = append(leaks, conn.String())
					}
				}
				if len(leaks) > 0 {
					return xerrors.Errorf(
						"TCP connections leaked.\nProcess id: %d\nPort: %d (%s)\nConnections:\n%v",
						pid, port, label, strings.Join(leaks, "\n"),
					)
				}
			}
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second*5), 5)); err != nil {
		if strings.Contains(err.Error(), "Unable to get connections") {
			logger.Log.Warn("Ignoring error: ", log.Error(err))
			return nil
		}
		return err
	}

	return nil

}

func InitConnectionResolver(connections map[string]connection.ManagedConnection) {
	stubResolver := connection.NewStubConnectionResolver()
	var err error
	for connID, conn := range connections {
		err = stubResolver.Add(connID, conn)
		if err != nil {
			panic(err)
		}
	}
	connection.Init(stubResolver)
}

// GetPortFromStr - works when the port is in the end of the string, preceded by a colon
func GetPortFromStr(s string) (int, error) {
	tokens := strings.Split(s, ":")
	if tokens[0] == s {
		return 1, xerrors.Errorf("Unable to find port in string %v (no colon)", s)
	}
	portStr := tokens[len(tokens)-1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 1, xerrors.Errorf("Unable to get port from string %v (unable to parse %v)", s, portStr)
	}
	return port, nil
}
