package raw_to_table_common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommonConfigValidate(t *testing.T) {
	testCases := []struct {
		name    string
		cfg     CommonConfig
		wantErr bool
	}{
		{
			name: "ValidConfig",
			cfg: CommonConfig{
				IsKeyEnabled: true,
				KeyType:      Bytes,
				ValueType:    String,
			},
			wantErr: false,
		},
		{
			name: "InvalidKeyType",
			cfg: CommonConfig{
				IsKeyEnabled: true,
				KeyType:      DataType("blablabla"),
				ValueType:    Bytes,
			},
			wantErr: true,
		},
		{
			name: "InvalidValueType",
			cfg: CommonConfig{
				IsKeyEnabled: true,
				KeyType:      Bytes,
				ValueType:    DataType("blablabla"),
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
