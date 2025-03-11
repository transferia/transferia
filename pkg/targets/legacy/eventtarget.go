package legacy

import (
	"runtime"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/base/events"
	"github.com/transferria/transferria/pkg/util/pool"
	"github.com/transferria/transferria/pkg/worker/tasks/cleanup"
	"go.ytsaurus.tech/library/go/core/log"
)

type legacyEventTarget struct {
	logger      log.Logger
	asyncSink   abstract.AsyncSink
	cleanupType model.CleanupType
	tmpPolicy   *model.TmpPolicyConfig
	pushQ       chan pushItem
	convertPool pool.Pool
}

type convertItem struct {
	batch  base.EventBatch
	doneCh chan convertResult
}

type convertResult struct {
	items         []abstract.ChangeItem
	cleanupTables abstract.TableMap
	err           error
}

type pushItem struct {
	convertCh chan convertResult
	resCh     chan error
}

func NewEventTarget(
	logger log.Logger,
	asyncSink abstract.AsyncSink,
	cleanupType model.CleanupType,
	tmpPolicy *model.TmpPolicyConfig) base.EventTarget {
	parallelism := runtime.GOMAXPROCS(0)
	t := &legacyEventTarget{
		logger:      logger,
		asyncSink:   asyncSink,
		cleanupType: cleanupType,
		tmpPolicy:   tmpPolicy,
		pushQ:       make(chan pushItem, parallelism),
		convertPool: nil,
	}
	t.convertPool = pool.NewDefaultPool(t.convert, uint64(parallelism))

	_ = t.convertPool.Run()
	go t.pusher()
	return t
}

func (t *legacyEventTarget) pusher() {
	for task := range t.pushQ {
		convRes := <-task.convertCh
		pushRes, err := t.push(convRes)
		if err != nil {
			task.resCh <- err
		} else if pushRes == nil {
			task.resCh <- nil
		} else {
			go func(inCh, outCh chan error) {
				outCh <- <-inCh
			}(pushRes, task.resCh)
		}
	}
}

func (t *legacyEventTarget) push(converted convertResult) (chan error, error) {
	if converted.err != nil {
		return nil, xerrors.Errorf("error converting to ChangeItems: %w", converted.err)
	}
	if c := len(converted.cleanupTables); c != 0 {
		t.logger.Infof("going to cleanup %d tables: %v", c, converted.cleanupTables)
		if err := cleanup.CleanupTables(t.asyncSink, converted.cleanupTables, t.cleanupType); err != nil {
			return nil, xerrors.Errorf("cannot cleanup (%s) tables in the target database: %w", string(t.cleanupType), err)
		}
	}
	if len(converted.items) == 0 {
		return nil, nil
	}
	return t.asyncSink.AsyncPush(converted.items), nil
}

func (t *legacyEventTarget) convert(in interface{}) {
	task := in.(convertItem)

	res := convertResult{
		items:         nil,
		cleanupTables: make(abstract.TableMap),
		err:           nil,
	}

	for task.batch.Next() {
		evt, err := task.batch.Event()
		if err != nil {
			res.err = xerrors.Errorf("error getting event from batch: %w", err)
			break
		}

		// TODO: Replace with TableLoad events handling
		if cleanupEvt, ok := evt.(events.CleanupEvent); ok {
			tableID := abstract.TableID(cleanupEvt)
			if t.tmpPolicy != nil && t.tmpPolicy.Include(tableID) {
				t.logger.Infof("skip cleanup for table '%v' included in tmp policy", tableID.Fqtn())
				continue
			}
			res.cleanupTables[tableID] = abstract.TableInfo{
				EtaRow: 0,
				IsView: false,
				Schema: nil,
			}
			continue
		}

		legacyEvt, ok := evt.(base.SupportsOldChangeItem)
		if !ok {
			continue
		}

		ci, err := legacyEvt.ToOldChangeItem()
		if err != nil {
			res.err = xerrors.Errorf("error converting event to change item: %w", err)
			break
		}
		res.items = append(res.items, *ci)
	}
	task.doneCh <- res
}

func (t *legacyEventTarget) AsyncPush(input base.EventBatch) chan error {
	resCh := make(chan error, 1)
	convCh := make(chan convertResult)

	if err := t.convertPool.Add(convertItem{
		batch:  input,
		doneCh: convCh,
	}); err != nil {
		resCh <- xerrors.Errorf("error putting batch to ChangeItems conversion pool: %w", err)
	}

	t.pushQ <- pushItem{
		convertCh: convCh,
		resCh:     resCh,
	}

	return resCh
}

func (t *legacyEventTarget) Close() error {
	close(t.pushQ)
	return t.asyncSink.Close()
}
