package tasks

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
)

type commitProbe struct {
	context context.Context
	commits int
	closes  int
}

var _ abstract.Committable = (*commitProbe)(nil)

func (s *commitProbe) Push([]abstract.ChangeItem) error { return nil }

func (s *commitProbe) Close() error {
	s.closes++
	return nil
}

func (s *commitProbe) Commit(ctx context.Context) error {
	s.context = ctx
	s.commits++
	return ctx.Err()
}

func TestEndDestinationPassesOperationContext(test *testing.T) {
	for _, v2 := range []bool{false, true} {
		for _, canceled := range []bool{false, true} {
			test.Run(fmt.Sprintf("v2=%t/canceled=%t", v2, canceled), func(test *testing.T) {
				sink := &commitProbe{context: nil, commits: 0, closes: 0}
				destination := new(snapshotOperationTestDestination)
				destination.SinkerFactory = func() abstract.Sinker { return sink }
				destination.Cleanup = model.DisabledCleanup
				transfer := new(model.Transfer)
				transfer.Src = new(model.MockSource)
				transfer.Dst = destination
				loader := NewSnapshotLoader(coordinator.NewStatefulFakeClient(), new(model.TransferOperation), transfer, solomon.NewRegistry(nil))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if canceled {
					cancel()
				}
				var err error
				if v2 {
					err = loader.endDestinationV2(ctx)
				} else {
					err = loader.endDestination(ctx)
				}
				if canceled {
					require.ErrorIs(test, err, context.Canceled)
				} else {
					require.NoError(test, err)
				}
				require.Same(test, ctx, sink.context)
				require.Equal(test, 1, sink.commits)
				require.Equal(test, 1, sink.closes)
			})
		}
	}
}
