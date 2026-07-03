package statictable

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	yt_sink "github.com/transferia/transferia/pkg/providers/yt/sink"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type WriterConfig struct {
	TransferID       string
	TxClient         yt.Tx
	Path             ypath.Path
	Spec             map[string]interface{}
	ChunkSize        int
	Logger           log.Logger
	Metrics          *stats.SinkerStats
	StringLimit      int
	DiscardBigValues bool
}

type Writer struct {
	tx yt.Tx

	writer yt.TableWriter

	logger     log.Logger
	rowsMetric func(rowCount int)

	stringLimit      int
	discardBigValues bool
}

func (w *Writer) Write(items []changeitem.ChangeItem) error {
	fastTableSchema := items[0].TableSchema.FastColumns()

	var skippedCount int
	for _, item := range items {
		if item.Kind != abstract.InsertKind {
			return xerrors.New("wrong change item kind for static table")
		}

		row := map[string]any{}
		for idx, col := range item.ColumnNames {
			colScheme, ok := fastTableSchema[abstract.ColumnName(col)]
			if !ok {
				return abstract.NewFatalError(xerrors.Errorf("unknown column name: %s", col))
			}
			var err error
			row[col], err = yt_sink.RestoreWithLengthLimitCheck(colScheme, item.ColumnValues[idx], w.discardBigValues, w.stringLimit)
			if err != nil {
				return xerrors.Errorf("cannot restore value for column '%s': %w", col, err)
			}
		}

		filteredRows := yt_sink.DiscardBigRowsIfNeeded([]any{row}, w.discardBigValues)
		if len(filteredRows) == 0 {
			w.logger.Warn("row size is greater than restriction", log.Any("table", item.Table))
			skippedCount += 1
			continue
		}

		if err := w.writer.Write(filteredRows[0]); err != nil {
			w.logger.Error("cannot write changeItem to static table", log.Any("table", item.Table),
				log.String("sub_tx_id", w.tx.ID().String()), log.Error(err))
			return err
		}
	}
	w.rowsMetric(len(items) - skippedCount)

	return nil
}

func (w *Writer) Commit() error {
	return w.writer.Commit()
}

func NewWriter(cfg WriterConfig) (*Writer, error) {
	tmpTablePath := makeTablePath(cfg.Path, cfg.TransferID, tmpNamePostfix)
	wr, err := yt.WriteTable(context.Background(), cfg.TxClient, tmpTablePath,
		yt.WithTableWriterConfig(cfg.Spec),
		yt.WithBatchSize(cfg.ChunkSize),
		yt.WithRetries(retriesCount),
		yt.WithExistingTable(),
		yt.WithAppend(),
	)
	if err != nil {
		return nil, err
	}

	return &Writer{
		tx:     cfg.TxClient,
		writer: wr,
		logger: cfg.Logger,

		rowsMetric: func(rowCount int) {
			cfg.Metrics.Table(cfg.Path.String(), "rows", rowCount)
		},
		stringLimit:      cfg.StringLimit,
		discardBigValues: cfg.DiscardBigValues,
	}, nil
}
