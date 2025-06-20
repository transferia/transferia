//go:build !disable_clickhouse_provider

package clickhouse

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/events"
	"github.com/transferia/transferia/pkg/providers"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/util"
)

type fakeTarget struct {
	mutex sync.Mutex
	items map[abstract.TableID][]abstract.ChangeItem
}

func (f *fakeTarget) AsyncPush(input base.EventBatch) chan error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	for input.Next() {
		ev, err := input.Event()
		if err != nil {
			return util.MakeChanWithError(xerrors.Errorf("unable to extract event: %w", err))
		}
		iev, ok := ev.(events.InsertEvent)
		if !ok {
			return util.MakeChanWithError(xerrors.Errorf("unexpected event type: %T", ev))
		}
		ci, err := iev.ToOldChangeItem()
		if err != nil {
			return util.MakeChanWithError(xerrors.Errorf("unable to constuct change event: %w", err))
		}
		if _, ok := f.items[ci.TableID()]; !ok {
			f.items[ci.TableID()] = make([]abstract.ChangeItem, 0)
		}
		f.items[ci.TableID()] = append(f.items[ci.TableID()], *ci)
	}
	return util.MakeChanWithError(nil)
}

func (f *fakeTarget) Close() error {
	return nil
}

func TestClickhouseProvider(t *testing.T) {
	src, err := chrecipe.Source(chrecipe.WithInitFile("gotest/dump.sql"))
	require.NoError(t, err)
	pr, err := NewClickhouseProvider(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), src, new(model.Transfer))
	require.NoError(t, err)
	require.NoError(t, pr.Init())
	objs, err := pr.DataObjects(nil)
	require.NoError(t, err)
	require.NoError(t, pr.BeginSnapshot())
	target := &fakeTarget{items: map[abstract.TableID][]abstract.ChangeItem{}}
	for objs.Next() {
		obj, err := objs.Object()
		require.NoError(t, err)
		for obj.Next() {
			part, err := obj.Part()
			require.NoError(t, err)
			ss, err := pr.CreateSnapshotSource(part)
			require.NoError(t, err)
			require.NoError(t, ss.Start(context.Background(), target))
			prog, err := ss.Progress()
			require.NoError(t, err)
			require.Equal(t, int(prog.Total()), int(prog.Current()))
			require.True(t, prog.Done())
		}
	}
	for t, data := range target.items {
		logger.Log.Infof("table: %v, sniff: \n%v", t.Fqtn(), abstract.Sniff(data))
		abstract.Dump(data)
	}
	require.Len(t, target.items, 4)
	require.NoError(t, pr.EndSnapshot())
	require.NoError(t, pr.Close())
}

func TestSwappedPortsConnectionClickhouseProvider(t *testing.T) {
	src, err := chrecipe.Source(chrecipe.WithInitFile("gotest/dump.sql"))
	require.NoError(t, err)

	src.NativePort, src.HTTPPort = src.HTTPPort, src.NativePort

	trf := new(model.Transfer)
	trf.Src = src

	if tester, ok := providers.Source[providers.Tester](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		trf,
	); ok {
		tr := tester.Test(context.Background())
		require.Errorf(t, tr.Err(), "unable to reach ClickHouse")
	}
}

func TestConnectionClickhouseProvider(t *testing.T) {
	src, err := chrecipe.Source(chrecipe.WithInitFile("gotest/dump.sql"))
	require.NoError(t, err)

	trf := new(model.Transfer)
	trf.Src = src

	if tester, ok := providers.Source[providers.Tester](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		trf,
	); ok {
		tr := tester.Test(context.Background())
		require.NoError(t, tr.Err())
	}
}
