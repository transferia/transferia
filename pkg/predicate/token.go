package predicate

// Token represents a lexical token.
type Token int

const (
	// ILLEGAL token represent illegal token found in the statement
	ILLEGAL Token = iota
	// EOF token represents end of statement
	EOF

	// Literals
	NUMBER // 12345.67
	STRING // "abc"
	TRUE   // true
	FALSE  // false

	operatorBegin
	AND // AND
	OR  // OR
	EQ  // =
	NEQ // !=
	LT  // <
	LTE // <=
	GT  // >
	GTE // >=
	operatorEnd

	LPAREN // (
	RPAREN // )

	NOT // NOT
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",

	NUMBER: "NUMBER",
	STRING: "STRING",
	TRUE:   "TRUE",
	FALSE:  "FALSE",

	AND: "AND",
	OR:  "OR",
	EQ:  "==",
	NEQ: "!=",
	LT:  "<",
	LTE: "<=",
	GT:  ">",
	GTE: ">=",

	NOT: "NOT",

	LPAREN: "(",
	RPAREN: ")",
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

// Precedence returns the operator precedence of the binary operator token.
func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2

	case EQ, NEQ, LT, LTE, GT, GTE:
		return 3
	}
	return 0
}

// isOperator returns true for operator tokens.
func (tok Token) isOperator() bool { return tok > operatorBegin && tok < operatorEnd }
