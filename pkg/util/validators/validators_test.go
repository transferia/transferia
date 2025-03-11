package validators

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/core/xerrors"
)

func TestHostPort(t *testing.T) {
	tests := []struct {
		name          string
		host          string
		port          string
		expectedError error
	}{
		{
			name: "Valid domain name host",
			host: "test-host1.com",
			port: "9092",
		},
		{
			name: "Valid ipv4 host",
			host: "129.144.50.56",
			port: "1234",
		},
		{
			name: "Valid ipv6 host",
			host: "2a02:6b8:b081:8116::1:36",
			port: "1234",
		},
		{
			name:          "Invalid comma separated list of hosts",
			host:          "test-host1.com,test-host2.com,test-host3.com",
			expectedError: xerrors.Errorf("invalid host: %q", "test-host1.com,test-host2.com,test-host3.com"),
		},
		{
			name:          "Invalid ipv6 host",
			host:          "2a02:6b8:bg081:8116::1:36",
			port:          "1234",
			expectedError: xerrors.Errorf("invalid host: %q", "2a02:6b8:bg081:8116::1:36"),
		},
		{
			name:          "Invalid domain name host, invalid character",
			host:          "test-host3.com@9092",
			expectedError: xerrors.Errorf("invalid host: %q", "test-host3.com@9092"),
		},
		{
			name:          "Invalid ipv4 host, invalid port",
			host:          "129.144.50.56",
			port:          "test",
			expectedError: xerrors.Errorf("could not parse port %q to int", "test"),
		},
		{
			name:          "Invalid ipv4 host, port to big",
			host:          "129.144.50.56",
			port:          "123456",
			expectedError: xerrors.Errorf("invalid port value %q > maxPortValue", "123456"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := HostPort(tc.host, tc.port)
			if tc.expectedError == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedError.Error())
			}
		})
	}
}
