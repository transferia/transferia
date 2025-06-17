//go:build !disable_clickhouse_provider

package db

type DDLFactory func(distributed bool, cluster string) (string, error)

type DDLExecutor interface {
	ExecDDL(fn DDLFactory) error
}
