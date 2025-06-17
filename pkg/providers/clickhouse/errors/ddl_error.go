//go:build !disable_clickhouse_provider

package errors

import "fmt"

type DDLTaskError struct {
	ExecShards  int
	TotalShards int
}

func (e DDLTaskError) Error() string {
	return fmt.Sprintf(
		"distributed DDL task is executed on %d shards of %d total, some cluster shards seems to be down or obsolete",
		e.ExecShards,
		e.TotalShards,
	)
}

func MakeDDLTaskError(exec, total int) DDLTaskError {
	return DDLTaskError{exec, total}
}
