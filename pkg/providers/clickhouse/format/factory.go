//go:build !disable_clickhouse_provider

package format

import (
	"io"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
)

type Validator interface {
	ReadAndValidate() (n int64, err error)
}

type FatalValidatorWithInputLogger struct {
	implValidator Validator
}

func (v *FatalValidatorWithInputLogger) ReadAndValidate() (int64, error) {
	n, err := v.implValidator.ReadAndValidate()
	if err != nil {
		return n, abstract.NewFatalError(xerrors.Errorf("%w", err))
	}
	return n, nil
}

func NewFatalValidator(in Validator) *FatalValidatorWithInputLogger {
	return &FatalValidatorWithInputLogger{
		implValidator: in,
	}
}

func newValidatorImpl(reader io.Reader, format model.ClickhouseIOFormat, expectedColumnsNum int) (Validator, error) {
	switch format {
	case model.ClickhouseIOFormatCSV:
		return NewCsvValidator(reader, expectedColumnsNum), nil
	case model.ClickhouseIOFormatJSONCompact:
		return NewJSONCompactValidator(reader, expectedColumnsNum), nil
	default:
		return nil, abstract.NewFatalError(xerrors.Errorf("unexpected format: %s, only CSV/JSONCompactEachRow supported", string(format)))
	}
}

func NewValidator(reader io.Reader, format model.ClickhouseIOFormat, expectedColumnsNum int) (*FatalValidatorWithInputLogger, error) {
	validator, err := newValidatorImpl(reader, format, expectedColumnsNum)
	if err != nil {
		return nil, err
	}
	return NewFatalValidator(validator), nil
}

func NewEvent(format model.ClickhouseIOFormat, row []byte, cols *abstract.TableSchema, names []string, table abstract.TableID, readerTime time.Time) (base.Event, error) {
	switch format {
	case model.ClickhouseIOFormatCSV:
		return NewCSVEvent(row, cols, names, table, readerTime), nil
	case model.ClickhouseIOFormatJSONCompact:
		return NewJSONCompactEvent(row, cols, names, table, readerTime), nil
	default:
		return nil, abstract.NewFatalError(xerrors.Errorf("unexpected format: %s, only CSV/JSONCompactEachRow supported", string(format)))
	}
}
