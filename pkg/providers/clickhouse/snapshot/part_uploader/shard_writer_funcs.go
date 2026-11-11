package part_uploader

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	ch_driver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	clickhouse_errors "github.com/transferia/transferia/pkg/providers/clickhouse/errors"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

// The returned function stops the timeout and waits for any cancellation already
// running, so it cannot affect the next operation.
func startIOTimeout(timeout time.Duration, cancel context.CancelFunc) func() {
	cancellationDone := make(chan struct{})
	timer := time.AfterFunc(
		timeout,
		func() {
			cancel()
			close(cancellationDone)
		},
	)
	return func() {
		if !timer.Stop() {
			<-cancellationDone
		}
	}
}

func buildInsertQuery(databaseName, tableName string, columnNames []string, includeTransferColumns bool) string {
	quotedColumnNames := make([]string, 0, len(columnNames)+2)
	for _, columnName := range columnNames {
		quotedColumnNames = append(quotedColumnNames, ch_eo_common.QuoteIdentifier(columnName))
	}
	if includeTransferColumns {
		quotedColumnNames = append(quotedColumnNames, "`__data_transfer_commit_time`", "`__data_transfer_delete_time`")
	}
	return "INSERT INTO " + ch_eo_common.QuoteIdentifier(databaseName) + "." + ch_eo_common.QuoteIdentifier(tableName) + " (" + strings.Join(quotedColumnNames, ",") + ")"
}

func appendRows(batch ch_driver.Batch, encoder *rowEncoder, items []abstract.ChangeItem) error {
	var appendError error
	func() {
		defer func() {
			if panicValue := recover(); panicValue != nil {
				appendError = abstract.NewFatalError(xerrors.Errorf("panic converting snapshot row: %v", panicValue))
			}
		}()

		for itemIndex := range items {
			arguments, err := encoder.prepareRowArguments(&items[itemIndex])
			if err != nil {
				appendError = err
				return
			}
			if err := batch.Append(arguments...); err != nil {
				appendError = err
				return
			}
		}
	}()
	return appendError
}

func wrapWriteError(operation string, err error) error {
	var conversionError *column.ColumnConverterError
	if clickhouse_errors.IsFatalClickhouseError(err) || xerrors.As(err, &conversionError) {
		return abstract.NewFatalError(xerrors.Errorf("%s: %w", operation, err))
	}
	return xerrors.Errorf("%s: %w", operation, err)
}

func estimateRowBytes(item *abstract.ChangeItem) uint64 {
	if item.Size.Values != 0 {
		return item.Size.Values
	}
	// Some sources omit sizes. Count their ch_eo_common payloads without reflection;
	// the row limit also bounds column metadata for empty or unusual values.
	rowBytes := uint64(len(item.ColumnValues)) * 16
	for _, value := range item.ColumnValues {
		switch typedValue := value.(type) {
		case string:
			rowBytes += uint64(len(typedValue))
		case []byte:
			rowBytes += uint64(len(typedValue))
		}
	}
	return rowBytes
}

// validateSourceSchemaColumnNames preserves the native sink's schema check: the
// source must keep the same number of columns in the same order.
func validateSourceSchemaColumnNames(expectedColumns, actualColumns []abstract.ColSchema) error {
	if len(expectedColumns) != len(actualColumns) {
		return abstract.NewFatalError(xerrors.Errorf("snapshot column count changed: expected %d, got %d", len(expectedColumns), len(actualColumns)))
	}
	for columnIndex := range expectedColumns {
		if expectedColumns[columnIndex].ColumnName != actualColumns[columnIndex].ColumnName {
			return abstract.NewFatalError(xerrors.Errorf("snapshot column %d changed: expected %q, got %q", columnIndex, expectedColumns[columnIndex].ColumnName, actualColumns[columnIndex].ColumnName))
		}
	}
	return nil
}
