package validator

import (
	"fmt"

	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/typesystem/values"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
)

type ValuesTypeCheckerSink struct {
	checkers map[schema.Type]values.ValueTypeChecker
}

func (t ValuesTypeCheckerSink) Close() error {
	return nil
}

func (t ValuesTypeCheckerSink) Push(items []abstract.ChangeItem) error {
	var errs util.Errors
	for _, row := range items {
		if !row.IsRowEvent() {
			logger.Log.Info("non-row event presented")
			continue
		}
		for i, name := range row.ColumnNames {
			for _, col := range row.TableSchema.Columns() {
				if col.ColumnName == name {
					if err := t.validValue(schema.Type(col.DataType), row.ColumnValues[i]); err != nil {
						errs = append(errs, xerrors.Errorf(
							"%s (type:%s, original:%s) value: %v (%T) failed: %w",
							col.ColumnName,
							col.DataType,
							col.OriginalType,
							util.Sample(fmt.Sprintf("%v", row.ColumnValues[i]), 100),
							row.ColumnValues[i],
							err,
						))
					} else {
						logger.Log.Debugf(
							"%s (type:%s, original:%s) valid value: %v (%T)",
							col.ColumnName,
							col.DataType,
							col.OriginalType,
							util.Sample(fmt.Sprintf("%v", row.ColumnValues[i]), 100),
							row.ColumnValues[i],
						)
					}
				}
			}
		}
	}
	if len(errs) > 0 {
		for _, err := range errs {
			logger.Log.Errorf("%v", err)
		}
		return abstract.NewFatalError(xerrors.Errorf("invalid items: %w", errs))
	}
	return nil
}

func (t ValuesTypeCheckerSink) validValue(typ schema.Type, val interface{}) error {
	if val == nil {
		return nil
	}
	checker, ok := t.checkers[typ]
	if !ok {
		return xerrors.Errorf("unknown typ: %s for %T", typ, val)
	}
	if !checker(val) {
		return xerrors.Errorf("incorrect go type %T for %s", val, typ)
	}
	return nil
}

func ValuesTypeChecker() abstract.Sinker {
	return &ValuesTypeCheckerSink{
		checkers: values.OneofValueTypeCheckers(),
	}
}

func valuesStrictTypeChecker() abstract.Sinker {
	return &ValuesTypeCheckerSink{
		checkers: values.StrictValueTypeCheckers(),
	}
}
