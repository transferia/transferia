package helpers

import (
	"os"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/shirou/gopsutil/v3/net"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
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
		connections, err := net.Connections("all")
		if err != nil {
			return xerrors.Errorf("Unable to get connections: %w", err)
		}

		for _, labeledPort := range labeledPorts {
			port, label := labeledPort.Port, labeledPort.Label
			if _, seen := visited[port]; !seen {
				visited[port] = true
				leaks := []string{}
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
