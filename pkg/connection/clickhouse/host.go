package clickhouse

type Host struct {
	Name       string
	NativePort int
	HTTPPort   int
	ShardName  string
}
