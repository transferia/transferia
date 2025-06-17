//go:build !disable_postgres_provider

package postgres

import (
	_ "embed"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/stats"
)

var (
	//go:embed testdata/hits_data.json
	fetcherData []byte
	//go:embed testdata/hits_binary_data.json
	fetcherBinaryData []byte
)

func TestCanonFetcherData(t *testing.T) {
	fetcher := new(ChangeItemsFetcher)
	stubData := &stubRows{iter: 0, limit: 1}
	require.NoError(t, json.Unmarshal(fetcherData, stubData))
	stubData.init()
	fetcher.connInfo = pgtype.NewConnInfo()
	fetcher.parseSchema = stubData.ParseSchema
	fetcher.rows = stubData
	fetcher.limitCount = 1
	fetcher.template = stubData.Template
	fetcher.unmarshallerData = UnmarshallerData{
		isHomo:   false,
		location: time.UTC,
	}
	fetcher.sourceStats = stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))

	fetcher.logger = logger.Log
	rows, err := fetcher.Fetch()
	require.NoError(t, err)
	require.Len(t, rows, 1)
}

func runB(b *testing.B, data []byte, limit int) {
	fetcher := new(ChangeItemsFetcher)
	stubData := &stubRows{iter: 0, limit: limit}
	require.NoError(b, json.Unmarshal(data, stubData))
	stubData.init()
	fetcher.connInfo = pgtype.NewConnInfo()
	fetcher.parseSchema = stubData.ParseSchema
	fetcher.rows = stubData
	fetcher.limitCount = limit
	fetcher.template = stubData.Template
	fetcher.unmarshallerData = UnmarshallerData{
		isHomo:   false,
		location: time.UTC,
	}
	fetcher.sourceStats = stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	fetcher.logger = logger.Log
	totalSize := 0
	for _, c := range stubData.Data[0] {
		totalSize += len(c)
	}

	for n := 0; n < b.N; n++ {
		stubData.iter = 0
		rows, err := fetcher.Fetch()
		require.NoError(b, err)
		require.Len(b, rows, limit)
		b.SetBytes(int64(totalSize * limit))
	}
	b.ReportAllocs()
}

func BenchmarkTextFetcher(b *testing.B) {
	b.Run("single_row", func(b *testing.B) {
		runB(b, fetcherData, 1)
	})
	b.Run("128_rows", func(b *testing.B) {
		runB(b, fetcherData, 128)
	})

	b.Run("single_binary_row", func(b *testing.B) {
		runB(b, fetcherBinaryData, 1)
	})
	b.Run("128_binary_rows", func(b *testing.B) {
		runB(b, fetcherBinaryData, 128)
	})
}
