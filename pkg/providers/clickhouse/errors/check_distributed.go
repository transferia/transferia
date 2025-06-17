//go:build !disable_clickhouse_provider

package errors

import (
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

const (
	// ClickhouseDDLTimeout is default value of distributed_ddl_task_timeout CH setting, used as fallback
	ClickhouseDDLTimeout = 180
	// DDLTimeoutCorrection is added to DDL query timeout to have a chance to catch CH error, instead of ContextDeadlineExceeded
	DDLTimeoutCorrection  = 2
	ClickhouseReadTimeout = 30 * time.Second
)

var ForbiddenDistributedDDLError = xerrors.New("detected Clickhouse cluster with multiple nodes and without distributed DDL support")

func IsDistributedDDLError(err error) bool {
	chError := new(clickhouse.Exception)
	if !xerrors.As(err, &chError) {
		return false
	}
	return (chError.Code == 139 && strings.Contains(chError.Message, "Zookeeper")) || // NO_ELEMENTS_IN_CONFIG error and no ZK setting
		(chError.Code == 225) || // NO_ZOOKEEPER
		(chError.Code == 392 && strings.Contains(chError.Message, "Distributed DDL")) || // QUERY_IS_PROHIBITED
		(chError.Code == 170 && strings.Contains(chError.Message, "cluster") && strings.Contains(chError.Message, "not found")) // Cluster not found, may be ignored if single node
}

type ErrDistributedDDLTimeout struct {
	ZKTaskPath string
	wrapped    error
}

// Unwrap implements xerrors.Uwrapper
func (e *ErrDistributedDDLTimeout) Unwrap() error {
	return e.wrapped
}

func (e *ErrDistributedDDLTimeout) Error() string {
	return e.wrapped.Error()
}

var distrTaskRe = regexp.MustCompile(`(/clickhouse/task_queue/ddl/[\w-]+)`)

// AsDistributedDDLTimeout checks whether err is clickhouse DDL timeout error and finds DDL task path
func AsDistributedDDLTimeout(err error) *ErrDistributedDDLTimeout {
	chError := new(clickhouse.Exception)
	// Distributed DDL Timeout has code 159 (TIMEOUT_EXCEEDED) and
	// message containing DDL task path in ZK and mentioning distributed_ddl_task_timeout setting value
	// Ex.: `failed to execute DDL "<ddl code>": code: 159, message: Watching task /clickhouse/task_queue/ddl/query-0000000015 is executing longer than distributed_ddl_task_timeout (=10) seconds. ...`
	if xerrors.As(err, &chError) && chError.Code == 159 && strings.Contains(chError.Message, "distributed_ddl_task_timeout") {
		if matches := distrTaskRe.FindStringSubmatch(chError.Message); len(matches) == 2 {
			return &ErrDistributedDDLTimeout{
				ZKTaskPath: matches[1],
				wrapped:    err,
			}
		}
	}
	return nil
}
