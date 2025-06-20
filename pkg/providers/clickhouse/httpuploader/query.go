//go:build !disable_clickhouse_provider

package httpuploader

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/multibuf"
	"github.com/transferia/transferia/pkg/util/pool"
)

type query = *multibuf.PooledMultiBuffer

func newInsertQuery(insertParams model.InsertParams, db string, table string, rowCount int, pool *sync.Pool) query {
	q := multibuf.NewPooledMultiBuffer(rowCount+1, pool)
	buf := q.AcquireBuffer(0)

	fmt.Fprintf(buf, "INSERT INTO `%s`.`%s` %s FORMAT JSONEachRow\n", db, table, insertParams.AsQueryPart())
	return q
}

func marshalQuery(batch []abstract.ChangeItem, rules *MarshallingRules, q query, avgRowSize int, parallelism uint64) error {
	var errs util.Errors
	type marshalTask struct {
		buf *bytes.Buffer
		row abstract.ChangeItem
	}

	taskPool := pool.NewDefaultPool(func(row interface{}) {
		task := row.(*marshalTask)
		if err := MarshalCItoJSON(task.row, rules, task.buf); err != nil {
			errs = util.AppendErr(errs, err)
		}
	}, parallelism)
	// Cannot just do defer taskPool.Close() because in case all tasks are succesfuly added to the pool,
	// pool.Close must be called before checking errs list. In this case defer must be canceled
	rb := util.Rollbacks{}
	defer rb.Do()
	rb.Add(func() { _ = taskPool.Close() })

	if err := taskPool.Run(); err != nil {
		return xerrors.Errorf("error running marshalling pool: %w", err)
	}
	for _, row := range batch {
		buf := q.AcquireBuffer(int(float64(avgRowSize) * MemReserveFactor))
		if err := taskPool.Add(&marshalTask{buf, row}); err != nil {
			return xerrors.Errorf("error adding marshalling task to pool: %w", err)
		}
	}

	_ = taskPool.Close()
	rb.Cancel()
	if len(errs) != 0 {
		return xerrors.Errorf("marshalling errors: %w", util.UniqueErrors(errs))
	}
	return nil
}
