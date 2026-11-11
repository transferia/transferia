package part_uploader

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

const defaultIOTimeout = 90 * time.Second

// OpenConnection creates a streaming connection with bounded socket I/O.
// The supplied options also configure metadata connections and stay unchanged.
func OpenConnection(options *clickhouse_go.Options) (driver.Conn, error) {
	writerOptions := *options
	writerOptions.MaxOpenConns, writerOptions.MaxIdleConns = 1, 1
	configureIOTimeouts(&writerOptions)
	connection, err := clickhouse_go.Open(&writerOptions)
	if err != nil {
		return nil, xerrors.Errorf("open snapshot native connection: %w", err)
	}
	return connection, nil
}

// A streaming INSERT can outlive one Push. Bound each network operation without
// expiring the batch while the source is idle. Stock Batch.Flush has no context
// watchdog, so a context timeout alone cannot interrupt a blocked socket write.
type timeoutConnection struct {
	net.Conn
	ioTimeout time.Duration
}

func newTimeoutConnection(connection net.Conn, ioTimeout time.Duration) *timeoutConnection {
	return &timeoutConnection{Conn: connection, ioTimeout: ioTimeout}
}

func (c *timeoutConnection) Read(buffer []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.ioTimeout)); err != nil {
		return 0, xerrors.Errorf("set snapshot read deadline: %w", err)
	}
	return c.Conn.Read(buffer)
}

func (c *timeoutConnection) Write(buffer []byte) (int, error) {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(c.ioTimeout)); err != nil {
		return 0, xerrors.Errorf("set snapshot write deadline: %w", err)
	}
	return c.Conn.Write(buffer)
}

// configureIOTimeouts bounds individual socket operations for streaming INSERTs.
func configureIOTimeouts(options *clickhouse_go.Options) {
	originalDialContext := options.DialContext
	tlsConfig := options.TLS
	dialTimeout := options.DialTimeout
	options.DialContext = func(ctx context.Context, address string) (net.Conn, error) {
		var connection net.Conn
		var err error
		switch {
		case originalDialContext != nil:
			connection, err = originalDialContext(ctx, address)
		case tlsConfig != nil:
			dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: dialTimeout}, Config: tlsConfig}
			connection, err = dialer.DialContext(ctx, "tcp", address)
		default:
			dialer := net.Dialer{Timeout: dialTimeout}
			connection, err = dialer.DialContext(ctx, "tcp", address)
		}
		if err != nil {
			return nil, xerrors.Errorf("dial snapshot native connection: %w", err)
		}
		return newTimeoutConnection(connection, defaultIOTimeout), nil
	}
}
