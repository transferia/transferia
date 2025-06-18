package clickhouse

import (
	"golang.org/x/sync/errgroup"

	"github.com/doublecloud/transfer/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	_ abstract.Sinker = (*ExactlyOnceSink)(nil)
)

type ExactlyOnceSink struct {
	sink  abstract.Sinker
	store InsertBlockStore
	lgr   log.Logger

	sm map[abstract.TablePartID]*deduper
}

func (e *ExactlyOnceSink) Close() error {
	return e.sink.Close()
}

func (e *ExactlyOnceSink) Push(items []abstract.ChangeItem) error {
	partitionBatches := map[abstract.TablePartID][]abstract.ChangeItem{}
	for _, item := range items {
		partitionBatches[item.TablePartID()] = append(partitionBatches[item.TablePartID()], item)
	}
	gr := errgroup.Group{}
	for part, batch := range partitionBatches {
		if _, ok := e.sm[part]; !ok {
			e.sm[part] = newDeduper(
				part,
				e.sink,
				e.store,
				e.lgr,
			)
		}
		gr.Go(e.sm[part].Process(batch))
	}
	return gr.Wait()
}

func NewExactlyOnce(sink abstract.Sinker, store InsertBlockStore, lgr log.Logger) *ExactlyOnceSink {
	return &ExactlyOnceSink{
		sink:  sink,
		store: store,
		lgr:   lgr,
		sm:    map[abstract.TablePartID]*deduper{},
	}
}
