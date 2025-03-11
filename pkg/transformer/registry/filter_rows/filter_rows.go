package filterrows

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/spf13/cast"
	parser "github.com/transferria/transferria/library/go/yandex/cloud/filter"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/transformer"
	"github.com/transferria/transferria/pkg/transformer/registry/filter"
	"github.com/transferria/transferria/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
	yts "go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/exp/constraints"
	"golang.org/x/xerrors"
)

const Type = abstract.TransformerType("filter_rows")

func init() {
	transformer.Register[Config](
		Type,
		func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewFilterRowsTransformer(cfg, lgr)
		},
	)
}

type termWithValues struct {
	Term      parser.Term
	ValuesSet *set.Set[interface{}] // not nil, when Term.Operator is IN/NOT IN
	ByteValue []byte                // not nil, when Term.Value.IsString()
}

type filteringExpression []termWithValues

type Config struct {
	Tables  filter.Tables `json:"tables"`
	Filter  string        `json:"filter"` // Deprecated: Use Filters instead.
	Filters []string      `json:"filters"`
}

func (c *Config) expressions() ([]filteringExpression, error) {
	if c.Filter != "" && len(c.Filters) > 0 {
		return nil, xerrors.New("Settings 'filters' and 'filter' cannot be enabled at the same time")
	}

	filters := []string{c.Filter}
	if len(c.Filters) > 0 {
		filters = c.Filters
	}
	result := make([]filteringExpression, len(filters))

	for i, filter := range filters {
		terms, err := parser.Parse(filter)
		if err != nil {
			return nil, xerrors.Errorf("Unable to parse filter '%s': %w", filter, err)
		}
		termsWithValues, err := prepareTermValues(terms)
		if err != nil {
			return nil, xerrors.Errorf("Unable to prepare term values: %w", err)
		}
		result[i] = termsWithValues
	}
	return result, nil
}

type FilterRowsTransformer struct {
	Tables      filter.Filter
	Logger      log.Logger
	Expressions []filteringExpression // ChangeItem transfers if it matches at least one of filters (expressions).

	impossibleKinds  *set.Set[abstract.Kind] // Apply() will create a fatal error when receiving such kind.
	appropriateKinds *set.Set[abstract.Kind] // Only appropriate kinds are filtered.
}

func NewFilterRowsTransformer(cfg Config, lgr log.Logger) (*FilterRowsTransformer, error) {
	tables, err := filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("Unable to init table filter: %w", err)
	}
	expressions, err := cfg.expressions()
	if err != nil {
		return nil, xerrors.Errorf("Unable to init filtering expressions: %w", err)
	}
	return &FilterRowsTransformer{
		Tables:           tables,
		Logger:           lgr,
		Expressions:      expressions,
		impossibleKinds:  set.New(abstract.UpdateKind, "Update", abstract.DeleteKind, "Delete"),
		appropriateKinds: set.New(abstract.InsertKind, "Insert"),
	}, nil
}

func (r *FilterRowsTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)
	for _, item := range input {
		if r.impossibleKinds.Contains(item.Kind) {
			err := abstract.NewFatalError(xerrors.Errorf("Found non-supported kind '%s'", item.Kind))
			errors = append(errors, abstract.TransformerError{Input: item, Error: err})
			continue
		}

		isNameMatching := filter.MatchAnyTableNameVariant(r.Tables, item.TableID())
		if !isNameMatching || abstract.IsSystemTable(item.TableID().Name) || !r.appropriateKinds.Contains(item.Kind) {
			transformed = append(transformed, item)
			continue
		}

		isMatched, err := r.matchItem(&item)
		if err != nil {
			errors = append(errors, abstract.TransformerError{Input: item, Error: abstract.NewFatalError(err)})
			continue
		}
		if isMatched {
			transformed = append(transformed, item)
		}
	}
	return abstract.TransformerResult{Transformed: transformed, Errors: errors}
}

func (r *FilterRowsTransformer) Type() abstract.TransformerType {
	return Type
}

// matchItem returns true if item matches at least one of filters and should be transferred.
func (r *FilterRowsTransformer) matchItem(item *abstract.ChangeItem) (bool, error) {
	for i, expression := range r.Expressions {
		isMatch, err := r.matchExpression(item, expression)
		if err != nil {
			return false, xerrors.Errorf("Unable to check expression %d: %w", i, err)
		}
		if isMatch {
			return true, nil
		}
	}
	return false, nil
}

func (r *FilterRowsTransformer) matchExpression(item *abstract.ChangeItem, terms filteringExpression) (bool, error) {
	for _, term := range terms {
		for i, columnName := range item.ColumnNames {
			// term.Attribute is a name of column which we are filtering by.
			if columnName != term.Term.Attribute {
				if i < len(item.ColumnNames)-1 {
					continue
				}
				tableName := item.TableID().Name
				return false, xerrors.Errorf("Unable to find column '%s' in table '%s'", term.Term.Attribute, tableName)
			}

			ok, err := matchValue(item.ColumnValues[i], term)
			if err != nil {
				info := fmt.Sprintf(
					"table: %s\n"+
						"column name: %s\n"+
						"table column type: %T\n"+
						"filter column type: %s",
					item.TableID().Name, columnName, item.ColumnValues[i], term.Term.Value.Type(),
				)
				return false, xerrors.Errorf("Unable to match value for %s:\nError: %w", info, err)
			}
			if !ok {
				return false, nil
			}
			// we can go to next term, because we have already found column for this term.
			break
		}
	}
	return true, nil
}

// matchValue checks if "val1 *op* val2" (e.g. val1 < val2) is true.
// val1 is column's value, val2 is filter's value.
func matchValue(val1 interface{}, term termWithValues) (bool, error) {
	val2 := term.Term.Value
	op := term.Term.Operator

	// In this function variables named like "type1" (e.g.: "bool1", "float1", "time1", ...)
	// stands for converted value of val1's type. Same for val2.

	// isInt1 will be true if val1 is int, same for isFloat1.
	isInt1, isFloat1 := false, false

	int1, err := toInt64E(val1)
	if err == nil {
		isInt1 = true
	} else if errors.Is(err, errIntOverflow) {
		return false, xerrors.Errorf("Unable to compare values: %w", errIntOverflow)
	}

	float1, err := cast.ToFloat64E(val1)
	// It is necessary to check that isInt1 == false. Otherwise cast.ToFloat64E can
	// convert int to float (1 --> 1.0) and we will have both isInt1 and isFloat1 true.
	if !isInt1 && err == nil {
		isFloat1 = true
	}

	// args: typeOfVal1, typeOfVal2, err
	errWrapStr := "Unable to match column's %s with filter's %s: %w"

	err = nil
	var isMatched bool
	switch {
	case val2.IsInt(), val2.IsIntList():
		if isInt1 {
			if isOperationWithSet(op) {
				isMatched, err = matchValueToSet(int1, term.ValuesSet, op)
			} else {
				isMatched, err = matchOrderedValue(int1, val2.AsInt(), op)
			}

			if err != nil {
				return false, xerrors.Errorf(errWrapStr, "Int", "Int", err)
			}
			return isMatched, nil
		}
		if isFloat1 {
			if isOperationWithSet(op) {
				if math.Trunc(float1) == float1 {
					isMatched, err = matchValueToSet(int64(float1), term.ValuesSet, op)
				} else {
					isMatched = false
				}
			} else {
				isMatched, err = matchOrderedValue(float1, float64(val2.AsInt()), op)
			}

			if err != nil {
				return false, xerrors.Errorf(errWrapStr, "Float", "Int", err)
			}
			return isMatched, nil
		}

	case val2.IsFloat(), val2.IsFloatList():
		if isInt1 {
			if isOperationWithSet(op) {
				isMatched, err = matchValueToSet(float64(int1), term.ValuesSet, op)
			} else {
				isMatched, err = matchOrderedValue(float64(int1), val2.AsFloat(), op)
			}

			if err != nil {
				return false, xerrors.Errorf(errWrapStr, "Int", "Float", err)
			}
			return isMatched, nil
		}
		if isFloat1 {
			if isOperationWithSet(op) {
				isMatched, err = matchValueToSet(float1, term.ValuesSet, op)
			} else {
				isMatched, err = matchOrderedValue(float1, val2.AsFloat(), op)
			}

			if err != nil {
				return false, xerrors.Errorf(errWrapStr, "Float", "Float", err)
			}
			return isMatched, nil
		}

	case val2.IsBool():
		// convert bool to int and compare
		if bool1, ok := val1.(bool); ok {
			int1, int2 := 0, 0
			if bool1 {
				int1 = 1
			}
			if val2.AsBool() {
				int2 = 1
			}
			isMatched, err := matchOrderedValue(int1, int2, op)
			if err != nil {
				return false, xerrors.Errorf(errWrapStr, "Bool", "Bool", err)
			}
			return isMatched, nil
		}

	case val2.IsString(), val2.IsStringList():
		byt, ok := val1.([]byte)
		if val2.IsString() && ok {
			if op == parser.Match {
				return bytes.Contains(byt, term.ByteValue), nil
			} else if op == parser.NotMatch {
				return !bytes.Contains(byt, term.ByteValue), nil
			}

			isMatched, err = matchBytesValue(byt, term.ByteValue, op)
			if err != nil {
				return false, xerrors.Errorf(errWrapStr, "String", "String", err)
			}
			return isMatched, nil
		}

		var str1 string
		if ok {
			str1 = string(byt)
		} else {
			str, isString := val1.(string)
			str1 = str
			ok = isString
		}
		if ok {
			if op == parser.Match {
				return strings.Contains(str1, val2.AsString()), nil
			} else if op == parser.NotMatch {
				return !strings.Contains(str1, val2.AsString()), nil
			}

			if isOperationWithSet(op) {
				isMatched, err = matchValueToSet(str1, term.ValuesSet, op)
			} else {
				isMatched, err = matchOrderedValue(str1, val2.AsString(), op)
			}

			if err != nil {
				return false, xerrors.Errorf(errWrapStr, "String", "String", err)
			}
			return isMatched, nil
		}

	case val2.IsTime(), val2.IsTimeList():
		if time1, ok := val1.(time.Time); ok {
			if isOperationWithSet(op) {
				isMatched, err = matchValueToSet(time1.UnixMicro(), term.ValuesSet, op)
			} else {
				isMatched, err = matchOrderedValue(time1.UnixMicro(), val2.AsTime().UnixMicro(), op)
			}
			if err != nil {
				return false, xerrors.Errorf(errWrapStr, "Time", "Time", err)
			}
			return isMatched, nil
		}
		// yt.TypeDate/yt.TypeDatetime/yt.TypeTimestamp can be stored as string in ChangeItem
		if str1, ok := val1.(string); ok {
			if time1, ok := stringToTime(str1); ok {
				if isOperationWithSet(op) {
					isMatched, err = matchValueToSet(time1.UnixMicro(), term.ValuesSet, op)
				} else {
					isMatched, err = matchOrderedValue(time1.UnixMicro(), val2.AsTime().UnixMicro(), op)
				}
				if err != nil {
					return false, xerrors.Errorf(errWrapStr, "Date", "Time", err)
				}
				return isMatched, nil
			}
		}

	case val2.IsNull():
		if op == parser.Equals {
			return val1 == nil, nil
		} else if op == parser.NotEquals {
			return val1 != nil, nil
		}

	default:
		return false, xerrors.New("Unknown filter's value type")
	}

	return false, xerrors.New("Unsupported type pair")
}

func matchOrderedValue[T constraints.Ordered](val1, val2 T, op parser.OperatorType) (bool, error) {
	switch op {
	case parser.Equals:
		return val1 == val2, nil
	case parser.NotEquals:
		return val1 != val2, nil
	case parser.Less:
		return val1 < val2, nil
	case parser.LessOrEquals:
		return val1 <= val2, nil
	case parser.Greater:
		return val1 > val2, nil
	case parser.GreaterOrEquals:
		return val1 >= val2, nil
	}
	return false, xerrors.Errorf("Unknown operation %s", op)
}

func matchBytesValue(val1, val2 []byte, op parser.OperatorType) (bool, error) {
	switch op {
	case parser.Equals:
		return bytes.Equal(val1, val2), nil
	case parser.NotEquals:
		return !bytes.Equal(val1, val2), nil
	case parser.Less:
		return bytes.Compare(val1, val2) < 0, nil
	case parser.LessOrEquals:
		return bytes.Compare(val1, val2) < 1, nil
	case parser.Greater:
		return bytes.Compare(val1, val2) > 0, nil
	case parser.GreaterOrEquals:
		return bytes.Compare(val1, val2) > -1, nil
	}
	return false, xerrors.Errorf("Unknown operation %s", op)
}

func matchValueToSet[T constraints.Ordered](val T, set *set.Set[interface{}], op parser.OperatorType) (bool, error) {
	if set == nil {
		return false, xerrors.Errorf("unable to check value matching without set, operator: %s", op.String())
	}

	switch op {
	case parser.In:
		return set.Contains(val), nil
	case parser.NotIn:
		return !set.Contains(val), nil
	}

	return false, xerrors.Errorf("Unknown set operation %s", op)
}

func prepareTermValues(terms []parser.Term) ([]termWithValues, error) {
	termsWithValues := make([]termWithValues, 0, len(terms))
	for _, term := range terms {
		currentTerm := termWithValues{
			Term:      term,
			ValuesSet: nil,
			ByteValue: nil,
		}

		if !isOperationWithSet(term.Operator) {
			if term.Value.IsString() {
				currentTerm.ByteValue = []byte(term.Value.AsString())
			}
		} else {
			set, err := valuesListToSet(term.Value)
			if err != nil {
				return nil, xerrors.Errorf("unable to prepare terms with list and byte values: %w", err)
			}

			currentTerm.ValuesSet = set
		}
		termsWithValues = append(termsWithValues, currentTerm)
	}

	return termsWithValues, nil
}

func isOperationWithSet(op parser.OperatorType) bool {
	return op == parser.In || op == parser.NotIn
}

func (r *FilterRowsTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	if !filter.MatchAnyTableNameVariant(r.Tables, table) {
		return false
	}
	cols := schema.Columns()
	for _, expression := range r.Expressions {
		isSuitable := r.checkExpressionSuitable(cols, expression)
		if !isSuitable {
			return false
		}
	}
	return true
}

func (r *FilterRowsTransformer) checkExpressionSuitable(cols []abstract.ColSchema, terms []termWithValues) bool {
	for _, term := range terms {
		for i, column := range cols {
			if term.Term.Attribute != column.ColumnName {
				if i < len(cols)-1 {
					continue
				}
				return false // term.Attribute not found in table
			}
			// found column by which we will filter
			if !r.checkColumnSuitable(term.Term.Value, column) {
				return false
			}
			// we can go to next term, because we have already found column for this term
			break
		}
	}
	return true
}

func (r *FilterRowsTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (r *FilterRowsTransformer) Description() string {
	return "Transformer for filtering rows by provided filter."
}

// checkColumnSuitable checks if column.DataType could be casted to parser.Value's type.
func (r *FilterRowsTransformer) checkColumnSuitable(value parser.Value, column abstract.ColSchema) bool {
	switch {
	case value.IsBool():
		return column.DataType == yts.TypeBoolean.String()

	case value.IsFloat(), value.IsInt():
		switch column.DataType {
		case yts.TypeInt8.String(), yts.TypeInt16.String(), yts.TypeInt32.String(), yts.TypeInt64.String(),
			yts.TypeUint8.String(), yts.TypeUint16.String(), yts.TypeUint32.String(), yts.TypeUint64.String(),
			yts.TypeFloat32.String(), yts.TypeFloat64.String():
			return true
		}

	case value.IsString():
		switch column.DataType {
		case yts.TypeString.String(), yts.TypeBytes.String(), yts.TypeAny.String():
			return true
		}

	case value.IsTime():
		switch column.DataType {
		case yts.TypeTimestamp.String(), yts.TypeDate.String(), yts.TypeDatetime.String():
			return true
		}

	case value.IsIntList(), value.IsFloatList(), value.IsStringList(), value.IsTimeList():
		return true

	case value.IsNull():
		return true
	}
	// Unmatched: yts.TypeBytes, yts.typeYSON, yts.TypeInterval
	return false
}
