//go:build !disable_ydb_provider

package logadapter

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	ydbLog "github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.ytsaurus.tech/library/go/core/log"
)

type Option = ydbLog.Option

func WithTraces(l log.Logger, d trace.Detailer, opts ...Option) ydb.Option {
	a := adapter{l: l}
	return ydb.MergeOptions(
		ydb.WithTraceDriver(ydbLog.Driver(a, d, opts...)),
		ydb.WithTraceTable(ydbLog.Table(a, d, opts...)),
		ydb.WithTraceScripting(ydbLog.Scripting(a, d, opts...)),
		ydb.WithTraceScheme(ydbLog.Scheme(a, d, opts...)),
		ydb.WithTraceCoordination(ydbLog.Coordination(a, d, opts...)),
		ydb.WithTraceRatelimiter(ydbLog.Ratelimiter(a, d, opts...)),
		ydb.WithTraceDiscovery(ydbLog.Discovery(a, d, opts...)),
		ydb.WithTraceTopic(ydbLog.Topic(a, d, opts...)),
		ydb.WithTraceDatabaseSQL(ydbLog.DatabaseSQL(a, d, opts...)),
	)
}
