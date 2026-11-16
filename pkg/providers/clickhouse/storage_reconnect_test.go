package clickhouse

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/dbaas"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
)

type reconnectHostsResolver struct {
	dbaas.ResolverFactory
	hosts []dbaas.ClusterHost
	err   error
	calls atomic.Int32
}

var _ dbaas.ResolverFactory = (*reconnectHostsResolver)(nil)
var _ dbaas.HostResolver = (*reconnectHostsResolver)(nil)
var _ dbaas.ShardGroupHostsResolver = (*reconnectHostsResolver)(nil)

func (r *reconnectHostsResolver) HostResolver(context.Context, dbaas.ProviderType, string) (dbaas.HostResolver, error) {
	return r, nil
}

func (r *reconnectHostsResolver) ResolveHosts(context.Context) ([]dbaas.ClusterHost, error) {
	r.calls.Add(1)
	return r.hosts, r.err
}

func (r *reconnectHostsResolver) ShardGroupHostsResolver(context.Context, dbaas.ProviderType, string) (dbaas.ShardGroupHostsResolver, error) {
	return r, nil
}

func (r *reconnectHostsResolver) ResolveHostsForShards(ctx context.Context, _ string, _ []string) ([]dbaas.ClusterHost, error) {
	return r.ResolveHosts(ctx)
}

// nativePingServer accepts Hello and answers Ping, or rejects the handshake with 516.
// This exercises the real native driver and SQL pool without implementing SELECT.
func nativePingServer(t *testing.T, address string, authenticationFailure bool) (string, int, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	// Count physical TCP connections, not Pings or discovery calls.
	accepted := new(atomic.Int32)
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go func() {
				defer connection.Close()
				_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
				reader := bufio.NewReader(connection)
				packet, err := reader.ReadByte()
				if err != nil || packet != 0 { // ClientHello
					return
				}
				readString := func() error {
					length, err := binary.ReadUvarint(reader)
					if err != nil {
						return err
					}
					_, err = io.CopyN(io.Discard, reader, int64(length))
					return err
				}
				if err := readString(); err != nil { // client name
					return
				}
				for i := 0; i < 3; i++ { // major, minor, protocol revision
					if _, err := binary.ReadUvarint(reader); err != nil {
						return
					}
				}
				for _, expected := range []string{"", "user", "password"} { // database, username, password
					length, err := binary.ReadUvarint(reader)
					if err != nil || length != uint64(len(expected)) {
						return
					}
					value := make([]byte, len(expected))
					if _, err := io.ReadFull(reader, value); err != nil || string(value) != expected {
						return
					}
				}
				if authenticationFailure {
					exception := binary.LittleEndian.AppendUint32([]byte{2}, 516) // ServerException
					for _, value := range []string{"DB::Exception", "Authentication failed", ""} {
						exception = binary.AppendUvarint(exception, uint64(len(value)))
						exception = append(exception, value...)
					}
					exception = append(exception, 0) // no nested exception
					_, _ = connection.Write(exception)
					return
				}
				hello := []byte{0, 10} // ServerHello, server name length
				hello = append(hello, []byte("ClickHouse")...)
				hello = binary.AppendUvarint(hello, 23)
				hello = binary.AppendUvarint(hello, 8)
				// An older supported revision avoids optional fields unrelated to failover.
				hello = binary.AppendUvarint(hello, 54032)
				if _, err := connection.Write(hello); err != nil {
					return
				}
				packet, err = reader.ReadByte()
				if err != nil || packet != 4 { // ClientPing
					return
				}
				_, _ = connection.Write([]byte{4}) // ServerPong
			}()
		}
	}()
	host, portString, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portString)
	require.NoError(t, err)
	if host == "::1" {
		host = "[::1]"
	}
	return host, port, accepted
}

// MDB replaces A with B after the first successful connection. A new physical
// SQL connection must use B instead of the address cached at storage creation.
func TestNativeReconnectUsesNewlyDiscoveredReplica(t *testing.T) {
	// Direct MDB endpoints share one configured port; IPv4/IPv6 give two distinct listeners.
	firstHost, port, firstConnections := nativePingServer(t, "127.0.0.1:0", false)
	secondHost, _, secondConnections := nativePingServer(t, net.JoinHostPort("::1", strconv.Itoa(port)), false)
	previous, _ := dbaas.Current()
	t.Cleanup(func() { dbaas.Init(previous) })
	resolver := &reconnectHostsResolver{hosts: []dbaas.ClusterHost{{Name: firstHost, ShardName: "shard", Health: dbaas.ALIVE}}}
	dbaas.Init(resolver)
	source := &clickhouse_model.ChSource{MdbClusterID: "cluster", User: "user", Password: "password", NativePort: port, HTTPPort: 8123}
	params, err := source.ToStorageParams()
	require.NoError(t, err)
	params.ConnectionParams.Secure = false
	db, err := makeShardConnection(params, "shard")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	// Prevent pool reuse so the second Ping necessarily opens a new connection.
	db.SetMaxIdleConns(0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))

	// Discovery changes only after A has already served a real native Ping.
	resolver.hosts = []dbaas.ClusterHost{{Name: secondHost, ShardName: "shard", Health: dbaas.ALIVE}}
	require.NoError(t, db.PingContext(ctx))
	require.Equal(t, int32(1), firstConnections.Load())
	require.Equal(t, int32(1), secondConnections.Load())
}

// MDB reports both replicas ALIVE, but A rejects the handshake with error 516.
// The driver must still try B rather than treating A's response as bad credentials everywhere.
func TestNativeConnectTriesHealthyReplicaAfterAuthenticationFailure(t *testing.T) {
	failedHost, port, failedConnections := nativePingServer(t, "127.0.0.1:0", true)
	healthyHost, _, healthyConnections := nativePingServer(t, net.JoinHostPort("::1", strconv.Itoa(port)), false)
	previous, _ := dbaas.Current()
	t.Cleanup(func() { dbaas.Init(previous) })
	dbaas.Init(&reconnectHostsResolver{hosts: []dbaas.ClusterHost{
		{Name: failedHost, ShardName: "shard", Health: dbaas.ALIVE},
		{Name: healthyHost, ShardName: "shard", Health: dbaas.ALIVE},
	}})
	params, err := (&clickhouse_model.ChSource{MdbClusterID: "cluster", User: "user", Password: "password", NativePort: port, HTTPPort: 8123}).ToStorageParams()
	require.NoError(t, err)
	params.ConnectionParams.Secure = false
	db, err := makeShardConnection(params, "shard")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	require.Equal(t, int32(1), failedConnections.Load())
	require.Equal(t, int32(1), healthyConnections.Load())
}
