package canon

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/providers/clickhouse"
	"github.com/transferria/transferria/pkg/providers/mongo"
	"github.com/transferria/transferria/pkg/providers/mysql"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/ydb"
	"github.com/transferria/transferria/pkg/providers/yt"
	"github.com/transferria/transferria/tests/canon/validator"
)

func TestAll(t *testing.T) {
	cases := All(
		ydb.ProviderType,
		yt.ProviderType,
		mongo.ProviderType,
		clickhouse.ProviderType,
		mysql.ProviderType,
		postgres.ProviderType,
	)
	for _, tc := range cases {
		t.Run(tc.String(), func(t *testing.T) {
			require.NotEmpty(t, tc.Data)
			snkr := validator.Referencer(t)()
			require.NoError(t, snkr.Push(tc.Data))
			require.NoError(t, snkr.Close())
		})
	}
}
