package predicate

import (
	"io"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/transferria/transferria/library/go/core/xerrors"
)

// Parser encapsulates the scanner and responsible for returning AST
// composed of statements read from a given reader.
type Parser struct {
	// Text scanner
	s *scanner.Scanner
	// Buffer to keep the read forward token
	buf struct {
		runeTok  rune   // last read token
		textTok  string // token text
		buffSize int    // buffer size (max=1)
	}
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	p := &Parser{
		s: new(scanner.Scanner),
		buf: struct {
			runeTok  rune
			textTok  string
			buffSize int
		}{},
	}
	p.s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanStrings
	p.s.Init(r)
	p.s.Error = func(_ *scanner.Scanner, msg string) {}
	return p
}

// Parse starts scanning & parsing process (main entry point).
// It returns an expression (AST) which you can use for the final evaluation
// of the conditions/statements
func (p *Parser) Parse() (Expr, error) {
	return p.parseExpr()
}

// scan returns the next token from the underlying scanner.
// If a token has been unscanned then read that instead.
func (p *Parser) scan() (rune, string) {
	// If we have a token on the buffer, then return it.
	if p.buf.buffSize != 0 {
		p.buf.buffSize = 0
	} else {
		// Otherwise read and put into buffer in case we 'unscan' it later
		p.buf.runeTok, p.buf.textTok = p.s.Scan(), p.s.TokenText()
	}
	return p.buf.runeTok, p.buf.textTok
}

// scanWithMapping uses scan with buffer (supports 'unscan') and maps
// scanner's tokens to our custom tokens.
func (p *Parser) scanWithMapping() (Token, string) {
	var (
		t   rune
		tok Token
		tt  string
	)

	t, tt = p.scan()

	// Map Go's token to our Token
	switch t {
	case scanner.EOF:
		tok = EOF
	case '(':
		tok = LPAREN
	case ')':
		tok = RPAREN
	case '-':
		t, _ = p.scan()

		if t == scanner.Float || t == scanner.Int {
			tok = NUMBER
			tt = "-" + tt
		} else {
			tok = ILLEGAL
		}
	case scanner.Float, scanner.Int:
		tok = NUMBER
	case '!':
		t, _ = p.scan()

		if t == '=' {
			tok = NEQ
			tt = "!="
		} else {
			tok = ILLEGAL
		}
	case '>':
		t, _ = p.scan()

		if t == '=' {
			tok = GTE
			tt = ">="
		} else {
			tok = GT
			tt = ">"
			p.unscan()
		}
	case '<':
		t, _ = p.scan()

		if t == '=' {
			tok = LTE
			tt = "<="
		} else {
			tok = LT
			tt = "<"
			p.unscan()
		}
	case '=':
		t, _ = p.scan()

		if t == '=' {
			tok = EQ
			tt = "=="
		} else {
			p.unscan()
			tok = EQ
		}

	case scanner.String, scanner.Char:
		tok = STRING
	case scanner.Ident:
		ttU := strings.ToUpper(tt)

		if ttU == "AND" {
			tok = AND
		} else if ttU == "OR" {
			tok = OR
		} else if ttU == "NOT" {
			_, tmp := p.scan()
			if tmp == "(" {
				p.unscan()
				tok = NOT
			}
		} else if ttU == "TRUE" {
			tok = TRUE
		} else if ttU == "FALSE" {
			tok = FALSE
		} else {
			tok = ILLEGAL
		}
	}

	return tok, tt
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() {
	p.buf.buffSize = 1
}

// parseExpr is an entry point to parsing
func (p *Parser) parseExpr() (Expr, error) {
	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	expr, err := p.parseUnaryExpr()
	if err != nil {
		return nil, xerrors.Errorf("unable to parse unary expr: %w", err)
	}

	// Loop over operations and unary exprs and build a tree based on precendence.
	for {
		// If the next token is NOT an operator then return the expression.
		op, tx := p.scanWithMapping()
		if op == ILLEGAL {
			return nil, xerrors.Errorf("ILLEGAL %s", tx)
		}
		if !op.isOperator() {
			p.unscan()
			return expr, nil

		}

		// Otherwise parse the next unary expression.
		rhs, err := p.parseUnaryExpr()
		if err != nil {
			return nil, xerrors.Errorf("unable to parse unary expr: %w", err)
		}

		// Assign the new root based on the precendence of the LHS and RHS operators.
		if lhs, ok := expr.(*BinaryExpr); ok && lhs.Op.Precedence() <= op.Precedence() {
			expr = &BinaryExpr{
				LHS: lhs.LHS,
				RHS: &BinaryExpr{LHS: lhs.RHS, RHS: rhs, Op: op},
				Op:  lhs.Op,
			}
		} else {
			expr = &BinaryExpr{LHS: expr, RHS: rhs, Op: op}
		}
	}

}

// parseUnaryExpr parses an non-binary expression.
func (p *Parser) parseUnaryExpr() (Expr, error) {
	// If the first token is a LPAREN then parse it as its own grouped expression.
	tok, lit := p.scanWithMapping()
	if tok == NOT {
		tok, _ = p.scanWithMapping()
		if tok != LPAREN {
			return nil, xerrors.Errorf("missing (")
		}
		expr, err := p.parseExpr()
		if err != nil {
			return nil, xerrors.Errorf("unable to parse expr: %w", err)
		}

		// Expect an RPAREN at the end.
		if tok, _ := p.scanWithMapping(); tok != RPAREN {
			return nil, xerrors.Errorf("missing )")
		}

		return &ParenExpr{Expr: expr, Inverted: true}, nil
	} else if tok == LPAREN {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, xerrors.Errorf("unable to parse expr: %w", err)
		}

		// Expect an RPAREN at the end.
		if tok, _ := p.scanWithMapping(); tok != RPAREN {
			return nil, xerrors.Errorf("missing )")
		}

		return &ParenExpr{Expr: expr, Inverted: false}, nil
	}

	// Read next token.
	switch tok {
	case STRING:
		return &StringLiteral{Val: lit[1 : len(lit)-1]}, nil
	case NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse number")
		}
		return &NumberLiteral{Val: v}, nil
	case TRUE, FALSE:
		return &BooleanLiteral{Val: tok == TRUE}, nil

	default:
		return nil, xerrors.Errorf("parsing error: runeTok=%v, lit=%v", tok, lit)
	}
}
