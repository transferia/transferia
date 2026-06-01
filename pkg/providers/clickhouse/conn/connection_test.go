package conn

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
)

var _ ConnParams = (*stubParams)(nil)

type stubParams struct{}

func (stubParams) User() string                     { return "user" }
func (stubParams) Password() string                 { return "pwd" }
func (stubParams) ResolvePassword() (string, error) { return "pwd", nil }
func (stubParams) Database() string                 { return "db" }
func (stubParams) SSLEnabled() bool                 { return false }
func (stubParams) PemFileContent() string           { return "" }
func (stubParams) RootCertPaths() []string          { return nil }
func (stubParams) ReadTimeout() time.Duration       { return 5 * time.Minute }

func TestGetClickhouseOptions_MultipleHostsPreserveOrder(t *testing.T) {
	hosts := []*chconn.Host{
		{Name: "klg-host.mdb.example", NativePort: 9440, HTTPPort: 8443},
		{Name: "sas-host.mdb.example", NativePort: 9440, HTTPPort: 8443},
		{Name: "vla-host.mdb.example", NativePort: 9440, HTTPPort: 8443},
	}

	opts, err := GetClickhouseOptions(stubParams{}, hosts)
	require.NoError(t, err)
	require.Equal(t, []string{
		"klg-host.mdb.example:9440",
		"sas-host.mdb.example:9440",
		"vla-host.mdb.example:9440",
	}, opts.Addr)
}

func TestGetClickhouseOptions_SingleHost(t *testing.T) {
	opts, err := GetClickhouseOptions(stubParams{}, []*chconn.Host{
		{Name: "only-host.example", NativePort: 9440},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"only-host.example:9440"}, opts.Addr)
}

// Reproduces the regression behind DTSUPPORT (https://st.yandex-team.ru/...):
// makeShardConnection used to pass only hosts[0] to ConnectNative, so a single
// unreachable replica killed the whole transfer. ConnectNative must surface
// every host to the driver verbatim.
func TestConnectNative_AllHostsLandInAddr(t *testing.T) {
	primary := &chconn.Host{Name: "primary.example", NativePort: 9440}
	replicas := []*chconn.Host{
		{Name: "replica-1.example", NativePort: 9440},
		{Name: "replica-2.example", NativePort: 9440},
	}

	opts, err := GetClickhouseOptions(stubParams{}, append([]*chconn.Host{primary}, replicas...))
	require.NoError(t, err)
	require.Equal(t, []string{
		"primary.example:9440",
		"replica-1.example:9440",
		"replica-2.example:9440",
	}, opts.Addr)
}
