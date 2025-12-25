package httpuploader

import (
	"context"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"go.ytsaurus.tech/library/go/core/log"
)

const MemReserveFactor = 1.2

var (
	httpClient    httpclient.HTTPClient
	httpClientMu  sync.Mutex
	tableBufPools map[string]*sync.Pool
	tableBufMu    sync.Mutex
)

func init() {
	tableBufPools = make(map[string]*sync.Pool)
}

func getHTTPClient(config conn.ConnParams) (httpclient.HTTPClient, error) {
	httpClientMu.Lock()
	defer httpClientMu.Unlock()
	if httpClient != nil {
		return httpClient, nil
	}
	cl, err := httpclient.NewHTTPClientImpl(config)
	if err != nil {
		return nil, err
	}
	httpClient = cl
	return httpClient, nil
}

type UploadStats struct {
	Bytes           int
	StartTime       time.Time
	UploadStartTime time.Time
}

// Insert row buffers should be pooled by tables as different tables may have different row size
func getPoolForTable(table string) *sync.Pool {
	tableBufMu.Lock()
	defer tableBufMu.Unlock()
	if memPool, ok := tableBufPools[table]; ok {
		return memPool
	}
	tableBufPools[table] = new(sync.Pool)
	return tableBufPools[table]
}

func UploadCIBatch(
	batch []abstract.ChangeItem,
	rules *MarshallingRules,
	config model.ChSinkServerParams,
	table string,
	avgRowSize int,
	lgr log.Logger) (*UploadStats, error) {
	stats := &UploadStats{
		Bytes:           0,
		StartTime:       time.Now(),
		UploadStartTime: time.Time{},
	}

	cl, err := getHTTPClient(config)
	if err != nil {
		return nil, xerrors.Errorf("error getting HTTP client: %w", err)
	}

	q := newInsertQuery(config.InsertSettings(), config.Database(), table, len(batch), getPoolForTable(table))
	defer q.Close()
	if err := marshalQuery(batch, rules, q, avgRowSize, uint64(runtime.GOMAXPROCS(0)*2)); err != nil {
		return nil, xerrors.Errorf("error marshalling rows to JSON: %w", err)
	}

	st := newReaderStats(500)
	rd := io.TeeReader(q, st)

	stats.UploadStartTime = time.Now()
	if err := cl.Exec(context.Background(), lgr, config.Host(), rd); err != nil {
		lgr.Error("Unable to insert", log.String("truncated_query", st.Sample()), log.Error(err))
		return nil, xerrors.Errorf("error executing CH query: %w", err)
	}
	stats.Bytes = st.Len()
	return stats, nil
}
