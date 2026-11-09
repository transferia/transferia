package mongo

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
)

type countingDialer struct {
	net.Dialer
	opened atomic.Int64
	open   atomic.Int64
}

func (d *countingDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	d.opened.Add(1)
	d.open.Add(1)
	return &countedConn{Conn: conn, open: &d.open}, nil
}

type countedConn struct {
	net.Conn
	open      *atomic.Int64
	closeOnce sync.Once
}

func (c *countedConn) Close() error {
	c.closeOnce.Do(func() { c.open.Add(-1) })
	return c.Conn.Close()
}

func TestWrongPassword(t *testing.T) {
	opts := RecipeSource().ConnectionOptions(nil)
	opts.Password = "wrong password"

	t.Run("newClient closes all connections", func(t *testing.T) {
		driverOpts, err := DriverConnectionOptions(&opts)
		require.NoError(t, err)
		dialer := new(countingDialer)
		driverOpts.SetDialer(dialer)

		client, err := newClient(context.Background(), driverOpts)
		require.Nil(t, client)
		require.True(t, error_codes.InvalidCredential.Contains(err), err)
		require.Greater(t, dialer.opened.Load(), int64(0))
		require.Eventually(t, func() bool { return dialer.open.Load() == 0 }, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("Connect keeps the code", func(t *testing.T) {
		client, err := Connect(context.Background(), opts, nil)
		require.Nil(t, client)
		require.True(t, error_codes.InvalidCredential.Contains(err), err)
	})
}
