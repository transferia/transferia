//go:build !disable_clickhouse_provider

package conn

type ConnParams interface {
	User() string
	Password() string
	ResolvePassword() (string, error)
	Database() string
	SSLEnabled() bool
	PemFileContent() string
	RootCertPaths() []string
}
