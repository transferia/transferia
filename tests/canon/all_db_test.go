package canon

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	"github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/canon/validator"
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
