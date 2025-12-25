package predicate

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type Operand struct {
	Op  Token
	Val any
}

func match[T string | float64](op Token, l, r T) bool {
	switch op {
	case EQ:
		return l == r
	case NEQ:
		return l != r
	case LT:
		return l < r
	case LTE:
		return l <= r
	case GT:
		return l > r
	case GTE:
		return l >= r
	}
	return false
}

func (o Operand) Match(v any) bool {
	switch vv := v.(type) {
	case string:
		return match(o.Op, vv, o.Val.(string))
	case float64:
		return match(o.Op, vv, o.Val.(float64))
	default:
		return false
	}
}

// Deprecated: Use library/go/yandex/cloud/filter instead, see TM-8537 for details.
func InclusionOperands(predicate abstract.WhereStatement, col string) ([]Operand, error) {
	if predicate == "" {
		return nil, nil
	}
	p := NewParser(strings.NewReader(string(predicate)))
	expr, err := p.Parse()
	if err != nil {
		return nil, xerrors.Errorf("unable to parse predicate: %s: %w", predicate, err)
	}
	return traverseOperands(expr, col, 0, nil, false), nil
}

func traverseOperands(expr Expr, col string, op Token, ops []Operand, inverted bool) []Operand {
	switch n := expr.(type) {
	case *ParenExpr:
		return traverseOperands(n.Expr, col, op, ops, n.Inverted)
	case *BinaryExpr:
		if n.Op == OR {
			return ops
		}
		l, ok := n.LHS.(*StringLiteral)
		if !ok {
			lops := traverseOperands(n.LHS, col, n.Op, ops, inverted)
			rops := traverseOperands(n.RHS, col, n.Op, ops, inverted)
			ops = append(ops, lops...)
			ops = append(ops, rops...)
			return ops
		} else if ok && l.Val != col {
			return ops
		}
		ops = append(ops, Operand{
			Op:  maybeInvert(n.Op, inverted),
			Val: findVal(n.RHS),
		})

		return ops
	}
	return ops
}

func maybeInvert(op Token, inverted bool) Token {
	if inverted {
		switch op {
		case EQ:
			return NEQ
		case NEQ:
			return EQ
		case LT:
			return GTE
		case LTE:
			return GT
		case GT:
			return LTE
		case GTE:
			return LT
		}
	}
	return op
}

func findVal(expr Expr) any {
	switch n := expr.(type) {
	case *NumberLiteral:
		return n.Val
	case *BooleanLiteral:
		return n.Val
	case *StringLiteral:
		return n.Val
	default:
		return ""
	}
}
