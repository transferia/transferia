package httpuploader

import (
	"context"
	"fmt"
	"sync"

	"github.com/transferia/transferia/pkg/abstract"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/util/multibuf"
	"golang.org/x/sync/errgroup"
)

type query = *multibuf.PooledMultiBuffer

func newInsertQuery(insertParams clickhouse_model.InsertParams, db string, table string, rowCount int, pool *sync.Pool) query {
	q := multibuf.NewPooledMultiBuffer(rowCount+1, pool)
	buf := q.AcquireBuffer(0)

	fmt.Fprintf(buf, "INSERT INTO `%s`.`%s` %s FORMAT JSONEachRow\n", db, table, insertParams.AsQueryPart())
	return q
}

func marshalQuery(batch []abstract.ChangeItem, rules *MarshallingRules, q query, avgRowSize int, parallelism uint64) error {
	eg, ctx := errgroup.WithContext(context.Background())
	eg.SetLimit(int(parallelism))
	for i := range batch {
		if ctx.Err() != nil {
			break
		}
		buf := q.AcquireBuffer(int(float64(avgRowSize) * MemReserveFactor))
		eg.Go(func() error { return MarshalCItoJSON(batch[i], rules, buf) })
	}

	return eg.Wait()
}
