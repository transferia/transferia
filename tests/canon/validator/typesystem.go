package validator

import (
	"fmt"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
)

type TypesystemCheckerSink struct {
	provider           abstract.ProviderType
	rules              map[string]schema.Type
	originalTypeParser func(colSchema abstract.ColSchema) string
}

func (t TypesystemCheckerSink) Close() error {
	return nil
}

func (t TypesystemCheckerSink) Push(items []abstract.ChangeItem) error {
	var errs util.Errors
	for _, row := range items {
		if !row.IsRowEvent() {
			logger.Log.Info("non-row event presented")
			continue
		}
		for i, name := range row.ColumnNames {
			for _, col := range row.TableSchema.Columns() {
				if col.ColumnName == name {
					if err := t.checkColType(col); err != nil {
						errs = append(errs, err)
					} else {
						logger.Log.Infof(
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

func (t TypesystemCheckerSink) checkColType(col abstract.ColSchema) error {
	originalType := t.originalTypeParser(col)
	if originalType == "" {
		return nil
	}
	var allowedOriginalTypes []string
	for orTyp, dtTyp := range t.rules {
		if dtTyp == schema.Type(col.DataType) {
			allowedOriginalTypes = append(allowedOriginalTypes, orTyp)
		}
	}
	for _, col := range allowedOriginalTypes {
		if col == originalType || col == typesystem.RestPlaceholder {
			return nil
		}
	}
	return xerrors.Errorf("col: %s type: %s(%s), not belong to type system rule: %v", col.ColumnName, col.DataType, originalType, allowedOriginalTypes)
}

func TypesystemChecker(provider abstract.ProviderType, originalTypeParser func(colSchema abstract.ColSchema) string) func() abstract.Sinker {
	return func() abstract.Sinker {
		return &TypesystemCheckerSink{
			provider:           provider,
			rules:              typesystem.RuleFor(provider).Source,
			originalTypeParser: originalTypeParser,
		}
	}
}
