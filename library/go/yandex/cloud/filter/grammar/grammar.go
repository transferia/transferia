package grammar

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	"github.com/transferria/transferria/library/go/core/xerrors"
)

// Filter syntax https://wiki.yandex-team.ru/cloud/devel/api/filtersyntax/

type Filter struct {
	HeadTerm  *Term      `parser:"[ WS ] [ @@ {"`
	TailTerms []*AndTerm `parser:"  @@ } ]"`
	Pos       lexer.Position
}

type AndTerm struct {
	And  *LogicOperator `parser:"[ WS ] @@ [ WS ]"`
	Term *Term          `parser:"@@"`
}

type LogicOperator struct {
	LogicOperator string `parser:"'AND'"`
}

type Term struct {
	Attribute string   `parser:"@Ident [ WS ]"`
	Operator  Operator `parser:"@( Operator | 'IN' | 'NOT' { WS } 'IN' )"`
	Value     *Value   `parser:"[ WS ] @@ [ WS ]"`
	Pos       lexer.Position
}

type Operator string

func (o *Operator) Capture(values []string) error {
	switch strings.ToUpper(values[0]) {
	case "IN":
		*o = "IN"
	case "NOT":
		*o = "NOT IN"
	default:
		*o = Operator(values[0])
	}
	return nil
}

type Value struct {
	String   *string   `parser:"@String"`
	DateTime *DateTime `parser:"| @DateTime"`
	Bool     *Bool     `parser:"| @('TRUE' | 'FALSE')"`
	Float    *Float    `parser:"| @Float"`
	Int      *Int      `parser:"| @Int"`
	Null     *Null     `parser:"| @('NULL' | 'NIL')"`
	List     []*Value  `parser:"| '(' [ WS ] @@ [ WS ] { [ WS ] ',' [ WS ] @@ [ WS ] } ')'"`
	Pos      lexer.Position
}

// delayedParse do post-parse parse
// participle provide useful Capture interface for such tasks,
// but it erase error types and our formatting.
// https://github.com/transferria/transferria/arc/trunk/arcadia/vendor/github.com/alecthomas/participle/nodes.go?rev=5800326#L37
func (v *Value) delayedParse() error {
	if v.DateTime != nil {
		return v.DateTime.delayedParse()
	}
	if v.Float != nil {
		return v.Float.delayedParse()
	}
	if v.Int != nil {
		return v.Int.delayedParse()
	}
	if v.Null != nil {
		return v.Null.delayedParse()
	}
	for _, subV := range v.List {
		if err := subV.delayedParse(); err != nil {
			return err
		}
	}
	return nil
}

func (v *Value) TypeOf() string {
	switch {
	case v.String != nil:
		return "string"
	case v.DateTime != nil:
		return "datetime"
	case v.Bool != nil:
		return "bool"
	case v.Float != nil:
		return "float"
	case v.Int != nil:
		return "int"
	case v.Null != nil:
		return "null"
	default:
		return "list"
	}
}

type Bool bool

func (b *Bool) Capture(values []string) error {
	*b = strings.ToUpper(values[0]) == "TRUE"
	return nil
}

func findTimeLayoutByMatchedTime(timePart string) string {
	if strings.Count(timePart, ":") == 2 {
		return "T15:04:05"
	}
	return "T15:04"
}

func findTimeLayout(value string) string {
	var timeLayout, tzLayout string
	timeIndex := strings.Index(value, "T")
	if timeIndex > 0 {
		timePart := value[timeIndex:]
		tzIndex := strings.IndexAny(timePart, "+-Z")
		if tzIndex < 0 {
			timeLayout = findTimeLayoutByMatchedTime(timePart)
		} else {
			// ts with timezone
			// 2006-01-02T15:04:05.999Z
			tzPart := timePart[tzIndex:]
			pureTimePart := timePart[:tzIndex]

			tzLayout = "Z07"
			if strings.Contains(tzPart, ":") {
				tzLayout = "Z07:00"
			}
			timeLayout = findTimeLayoutByMatchedTime(pureTimePart)
		}
	}

	return "2006-01-02" + timeLayout + tzLayout
}

type DateTime struct {
	T     time.Time
	value string
	Pos   lexer.Position
}

type SyntaxError struct {
	Message string
	Pos     lexer.Position
}

func (se *SyntaxError) Error() string {
	return se.Message
}

func newSyntaxError(pos lexer.Position, fromError error) error {
	return &SyntaxError{Pos: pos, Message: fromError.Error()}
}

func (dt *DateTime) Capture(values []string) error {
	dt.value = strings.Join(values, "")
	return nil
}

// delayedParse parse Time from captured string
func (dt *DateTime) delayedParse() error {
	// This also works for parsing time without a time zone,
	// since `time.Parse` uses UTC for such a case
	//
	//	"In the absence of a time zone indicator, Parse returns a time in UTC."
	parsed, err := time.Parse(findTimeLayout(dt.value), dt.value)
	if err != nil {
		return newSyntaxError(dt.Pos, err)
	}
	dt.T = parsed
	return nil
}

type Float struct {
	F     float64
	value string
	Pos   lexer.Position
}

func (f *Float) Capture(values []string) error {
	f.value = values[0]
	return nil
}

// delayedParse parse string into number
func (f *Float) delayedParse() error {
	fv, err := strconv.ParseFloat(f.value, 64)
	if err != nil {
		// strip function name from error
		var numError *strconv.NumError
		if xerrors.As(err, &numError) {
			return newSyntaxError(f.Pos, numError.Err)
		}
		return err
	}
	f.F = fv
	return nil
}

type Int struct {
	I     int64
	value string
	Pos   lexer.Position
}

func (i *Int) Capture(values []string) error {
	i.value = values[0]
	return nil
}

// delayedParse parse string into number
func (i *Int) delayedParse() error {
	iv, err := strconv.ParseInt(i.value, 10, 64)
	if err != nil {
		// strip function name from error
		var numError *strconv.NumError
		if xerrors.As(err, &numError) {
			return newSyntaxError(i.Pos, numError.Err)
		}
		return err
	}
	i.I = iv
	return nil
}

type Null struct {
	V     bool
	value string
	Pos   lexer.Position
}

func (i *Null) Capture(values []string) error {
	i.value = strings.ToUpper(values[0])
	return nil
}

// delayedParse parse string into value
func (i *Null) delayedParse() error {
	if i.value == "NULL" || i.value == "NIL" {
		i.V = true
	}
	return nil
}

var (
	filterLexer = lexer.Must(lexer.Regexp(
		`(?P<Operator>!=|<=|>=|!~|[=<>~])` +
			`|(?P<String>'((\\'|[^']))*'|"(\\"|[^"])*")` +
			`|(?P<DateTime>\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d+(:\d+)?)?)?)` +
			`|(?P<Ident>[a-zA-Z][a-zA-Z0-9_.]*)` +
			`|(?P<Float>[-+]?\d+\.\d+)` +
			`|(?P<Int>[-+]?\d+)` +
			`|(?P<Punctuation>[(),])` +
			`|(?P<WS>\s+)`,
	))
	filterParser = participle.MustBuild(
		&Filter{},
		participle.Lexer(filterLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Ident"),
		participle.UseLookahead(0),
	)
)

func Parse(filters string) ([]Term, error) {
	if filters == "" {
		return nil, nil
	}
	flt := Filter{}
	if err := filterParser.ParseString(filters, &flt); err != nil {
		var lexError *lexer.Error
		if xerrors.As(err, &lexError) {
			return nil, &SyntaxError{
				Message: lexError.Message,
				Pos:     lexError.Pos,
			}
		}
		var tokenError participle.UnexpectedTokenError
		if xerrors.As(err, &tokenError) {
			return nil, &SyntaxError{
				Message: fmt.Sprintf("unexpected token %q", tokenError.Value),
				Pos:     tokenError.Pos,
			}
		}
		return nil, err
	}

	var ret []Term
	if flt.HeadTerm != nil {
		ret = append(ret, *flt.HeadTerm)
	}
	for _, term := range flt.TailTerms {
		ret = append(ret, *term.Term)
	}

	for _, term := range ret {
		if err := term.Value.delayedParse(); err != nil {
			return nil, err
		}
	}

	return ret, nil
}
