//go:build !disable_yt_provider

package bechmarks

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	client2 "github.com/transferia/transferia/pkg/abstract/coordinator"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/sink"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type overrideable interface {
	OverrideClient(client yt.Client)
}

type fakeYTTX struct {
	yt.TabletTx
}

func (fakeYTTX) InsertRows(
	ctx context.Context,
	path ypath.Path,
	rows []any,
	options *yt.InsertRowsOptions,
) (err error) {
	return nil
}

func (fakeYTTX) Abort() error {
	return nil
}

func (fakeYTTX) Commit() error {
	return nil
}

type fakeYT struct {
	yt.Client
	cols []schema.Column
}

func (fakeYT) NodeExists(
	ctx context.Context,
	path ypath.YPath,
	options *yt.NodeExistsOptions,
) (ok bool, err error) {
	return true, nil
}

func (fakeYT) BeginTabletTx(ctx context.Context, options *yt.StartTabletTxOptions) (tx yt.TabletTx, err error) {
	return &fakeYTTX{}, nil
}

func (f fakeYT) GetNode(
	ctx context.Context,
	path ypath.YPath,
	result any,
	options *yt.GetNodeOptions,
) (err error) {
	resPtr, ok := result.(*struct {
		Schema      schema.Schema `yson:"schema"`
		TabletState string        `yson:"expected_tablet_state"`
	})
	if !ok {
		return xerrors.Errorf("result must be a pointer to the expected struct")
	}

	resPtr.TabletState = yt.TabletMounted
	resPtr.Schema = schema.Schema{
		Strict:     aws.Bool(true),
		UniqueKeys: true,
		Columns:    f.cols,
	}

	return nil
}

func BenchmarkSinkWrite(b *testing.B) {
	scenario := func(b *testing.B, table abstract.Sinker, size int, ci abstract.ChangeItem) {
		var data []abstract.ChangeItem
		for range size {
			data = append(data, ci)
		}
		err := table.Push(data)
		b.SetBytes(int64(ci.Size.Values) * int64(size))
		require.NoError(b, err)
	}

	b.Run("simple", func(b *testing.B) {
		schema_ := abstract.NewTableSchema([]abstract.ColSchema{
			{
				DataType:   "double",
				ColumnName: "test",
				PrimaryKey: true,
			},
			{
				DataType:   "datetime",
				ColumnName: "_timestamp",
				PrimaryKey: true,
			},
		})
		row := abstract.ChangeItem{
			TableSchema:  schema_,
			Table:        "test",
			Kind:         "insert",
			ColumnNames:  []string{"test", "_timestamp"},
			ColumnValues: []interface{}{3.99, time.Now()},
		}
		b.Run("dt_hack", func(b *testing.B) {
			cfg := yt2.NewYtDestinationV1(yt2.YtDestination{
				CellBundle:          "default",
				PrimaryMedium:       "default",
				DisableDatetimeHack: false,
				CanAlter:            true,
			})
			cfg.WithDefaults()
			table, err := sink.NewSinker(cfg, "some_uniq_transfer_id", logger.LoggerWithLevel(zapcore.WarnLevel), metrics.NewRegistry(), client2.NewFakeClient(), nil)
			require.NoError(b, err)
			if o, ok := table.(overrideable); ok {
				o.OverrideClient(&fakeYT{cols: []schema.Column{{
					Name:      "test",
					Type:      "double",
					SortOrder: "ascending",
				}, {
					Name:      "_timestamp",
					Type:      "int64",
					SortOrder: "ascending",
				}, {
					Name: "__dummy",
					Type: "any",
				}}})
			}
			b.Run("10_000", func(b *testing.B) {
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					scenario(b, table, 10_000, row)
				}
				b.ReportAllocs()
			})
		})
		b.Run("no_dt_hack", func(b *testing.B) {
			cfg := yt2.NewYtDestinationV1(yt2.YtDestination{
				CellBundle:          "default",
				PrimaryMedium:       "default",
				DisableDatetimeHack: true,
				CanAlter:            true,
			})
			cfg.WithDefaults()
			table, err := sink.NewSinker(cfg, "some_uniq_transfer_id", logger.LoggerWithLevel(zapcore.WarnLevel), metrics.NewRegistry(), client2.NewFakeClient(), nil)
			require.NoError(b, err)
			if o, ok := table.(overrideable); ok {
				o.OverrideClient(&fakeYT{cols: []schema.Column{{
					Name:      "test",
					Type:      "double",
					SortOrder: "ascending",
				}, {
					Name:      "_timestamp",
					Type:      "datetime",
					SortOrder: "ascending",
				}, {
					Name: "__dummy",
					Type: "any",
				}}})
			}
			b.Run("10_000", func(b *testing.B) {
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					scenario(b, table, 10_000, row)
				}
				b.ReportAllocs()
			})
		})
	})
}
