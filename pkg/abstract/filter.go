package abstract

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type Filters map[Table]WhereStatement

var pairs = map[rune]rune{'(': ')'}

type Table string

type WhereStatement string

const NoFilter WhereStatement = WhereStatement("")

// bracketType sorts characters into either opening, closing, or not a bracket
type bracketType int

// TestVersion is the version of unit tests that this will pass
const TestVersion = 2

const (
	openBracket bracketType = iota
	closeBracket
	notABracket
)

func BalanceBrackets(phrase string) bool {
	var queue []rune
	for _, v := range phrase {
		switch getBracketType(v) {
		case openBracket:
			queue = append(queue, pairs[v])
		case closeBracket:
			if 0 < len(queue) && queue[len(queue)-1] == v {
				queue = queue[:len(queue)-1]
			} else {
				return false
			}
		}
	}
	return len(queue) == 0
}

func getBracketType(char rune) bracketType {
	for k, v := range pairs {
		switch char {
		case k:
			return openBracket
		case v:
			return closeBracket
		}
	}
	return notABracket
}

func ParseFilter(rawFilter string) (*Table, *WhereStatement, error) {
	if len(rawFilter) == 0 {
		return nil, nil, nil
	}

	if strings.Count(rawFilter, "(") == 0 {
		return nil, nil, xerrors.New("Need at least one open bracket")
	}

	if strings.Count(rawFilter, ")") == 0 {
		return nil, nil, xerrors.New("Need at least one closed bracket")
	}

	if !BalanceBrackets(rawFilter) {
		return nil, nil, xerrors.New("Brackets not balanced")
	}

	left := strings.IndexRune(rawFilter, '(')
	if rawFilter[len(rawFilter)-1:] != ")" {
		return nil, nil, xerrors.New("Must end with closing bracket")
	}
	table := Table(rawFilter[0:left])
	filter := WhereStatement(rawFilter[left+1 : len(rawFilter)-1])
	return &table, &filter, nil
}

func ParseFilterItems(rawFilters []string) (Filters, error) {
	res := make(map[Table]WhereStatement)
	for _, filter := range rawFilters {
		table, filter, err := ParseFilter(filter)
		if err != nil {
			return nil, err
		}

		if table == nil || filter == nil {
			continue
		}

		res[*table] = *filter
	}

	return res, nil
}

func FiltersIntersection(a WhereStatement, b WhereStatement) WhereStatement {
	if a == NoFilter {
		return b
	}
	if b == NoFilter {
		return a
	}
	return WhereStatement("(" + a + ") AND (" + b + ")")
}

func NotStatement(a WhereStatement) WhereStatement {
	return WhereStatement(fmt.Sprintf("NOT (%s)", a))
}
