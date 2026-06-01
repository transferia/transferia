package conn

import "time"

type ConnParams interface {
	User() string
	Password() string
	ResolvePassword() (string, error)
	Database() string
	SSLEnabled() bool
	PemFileContent() string
	RootCertPaths() []string
	ReadTimeout() time.Duration
}
