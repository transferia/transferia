package reader

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

var nginxVarRegexp = regexp.MustCompile(`\$([A-Za-z0-9_]+)`)

// multilineCollapseRegexp matches any spaces before and after of '\n'.
// Used to properly collapse '\n' in format to single space (' ') to normalize format.
var multilineCollapseRegexp = regexp.MustCompile(`[ \t]*\n[ \t]*`)

// nginxWellKnownTokenTypes maps well-known nginx variable names to their data types.
// Variables not found here default to schema.TypeString.
//
// NOTE: Some numeric tokens (e.g. upstream_status, upstream_response_time and others) are typed as String because nginx
// separates values from multiple upstream attempts with commas (e.g. "0.1, 0.2"), which cannot be parsed as a single number.
// See https://nginx.org/en/docs/http/ngx_http_upstream_module.html
var nginxWellKnownTokenTypes = map[string]schema.Type{
	"status":              schema.TypeInt64,
	"server_port":         schema.TypeInt64,
	"body_bytes_sent":     schema.TypeInt64,
	"bytes_sent":          schema.TypeInt64,
	"request_length":      schema.TypeInt64,
	"connection":          schema.TypeInt64,
	"connection_requests": schema.TypeInt64,
	"request_time":        schema.TypeFloat64,
	"msec":                schema.TypeFloat64,
	"time_local":          schema.TypeDatetime,
}

type nginxToken struct {
	IsVariable bool
	Value      string // Value is name without $ if IsVariable, literal text otherwise.
}

type nginxField struct {
	Name    string
	ColType schema.Type
}

// compiledNginxFormat holds the tokenized format and field metadata for parsing nginx log entries.
// Parsing works by walking the token sequence: literals are matched exactly (with whitespace
// flexibility), variables are captured up to the next literal's delimiter character.
type compiledNginxFormat struct {
	tokens []nginxToken
	fields []nginxField
	schema []abstract.ColSchema
}

// tokenizeFormat splits the format string into alternating literal and variable segments.
func tokenizeFormat(format string) []nginxToken {
	format = strings.TrimSpace(format)
	format = multilineCollapseRegexp.ReplaceAllString(format, " ") // Collapse newlines to spaces.
	var tokens []nginxToken
	lastEnd := 0
	allMatches := nginxVarRegexp.FindAllStringSubmatchIndex(format, -1)
	for _, loc := range allMatches {
		if loc[0] > lastEnd {
			tokens = append(tokens, nginxToken{IsVariable: false, Value: format[lastEnd:loc[0]]})
		}
		tokens = append(tokens, nginxToken{IsVariable: true, Value: format[loc[2]:loc[3]]})
		lastEnd = loc[1]
	}
	if lastEnd < len(format) {
		tokens = append(tokens, nginxToken{IsVariable: false, Value: format[lastEnd:]})
	}
	return tokens
}

// compileFormat compiles a nginx log_format string into a token-based parser.
func compileFormat(format string) (*compiledNginxFormat, error) {
	tokens := tokenizeFormat(format)
	if len(tokens) == 0 {
		return nil, xerrors.New("empty nginx format string")
	}

	hasVariable := false
	for _, tok := range tokens {
		if tok.IsVariable {
			hasVariable = true
			break
		}
	}
	if !hasVariable {
		return nil, xerrors.New("nginx format string contains no variables")
	}

	var fields []nginxField
	usedNames := make(map[string]int)

	for _, tok := range tokens {
		if !tok.IsVariable {
			continue
		}

		colName := makeUniqueColumnName(tok.Value, usedNames)
		colType, ok := nginxWellKnownTokenTypes[tok.Value]
		if !ok {
			colType = schema.TypeString
		}
		fields = append(fields, nginxField{
			Name:    colName,
			ColType: colType,
		})
	}

	schemaCols := make([]abstract.ColSchema, 0, len(fields))
	for idx, f := range fields {
		col := abstract.NewColSchema(f.Name, f.ColType, false)
		col.Path = strconv.Itoa(idx)
		col.OriginalType = fmt.Sprintf("nginx:%s", f.ColType.String())
		schemaCols = append(schemaCols, col)
	}

	return &compiledNginxFormat{
		tokens: tokens,
		fields: fields,
		schema: schemaCols,
	}, nil
}

// parseEntry parses one log entry from input by walking the format tokens.
// Returns extracted field values and the number of bytes consumed from input.
func (c *compiledNginxFormat) parseEntry(input string) ([]string, int, error) {
	values := make([]string, len(c.fields))
	pos := 0
	fieldIdx := 0

	for i, tok := range c.tokens {
		if !tok.IsVariable {
			n := matchLiteral(input[pos:], tok.Value)
			if n < 0 {
				format := "format mismatch at position %d: expected %q, got %q"
				return nil, 0, xerrors.Errorf(format, pos, tok.Value, input[pos:])
			}
			pos += n
			continue
		}

		// Variable: read until the delimiter (first char of next literal).
		delimiter := c.nextDelimiter(i)
		var end int
		if delimiter == 0 {
			// Last variable with no following literal — read to newline or end of input.
			end = indexOfNewline(input[pos:])
			if end < 0 {
				end = len(input) - pos
			}
		} else {
			end = findDelimiter(input[pos:], delimiter)
			if end < 0 {
				format := "cannot find delimiter '%c' after $%s at position %d"
				return nil, 0, xerrors.Errorf(format, delimiter, tok.Value, pos)
			}
		}

		values[fieldIdx] = input[pos : pos+end]
		pos += end
		fieldIdx++
	}

	return values, pos, nil
}

// nextDelimiter returns the first character of the next literal token after afterIndex.
// Returns 0 if there is no following literal (i.e. the variable is the last token).
func (c *compiledNginxFormat) nextDelimiter(afterIndex int) byte {
	for j := afterIndex + 1; j < len(c.tokens); j++ {
		if !c.tokens[j].IsVariable && len(c.tokens[j].Value) > 0 {
			return c.tokens[j].Value[0]
		}
	}
	return 0
}

// matchLiteral matches a format literal against input character by character.
// Spaces in the format match one or more whitespace characters in the input,
// allowing a single format to match both single-line and multi-line log entries.
// Returns the number of bytes consumed from input, or -1 on mismatch.
func matchLiteral(input, literal string) int {
	inputPos := 0
	for i := 0; i < len(literal); i++ {
		if inputPos >= len(input) {
			return -1
		}
		lch := literal[i]
		if lch == ' ' {
			if !isWhitespace(input[inputPos]) {
				return -1
			}
			// Consume all consecutive whitespace (handles \r\n, multiple spaces, etc.)
			for inputPos < len(input) && isWhitespace(input[inputPos]) {
				inputPos++
			}
		} else {
			if input[inputPos] != lch {
				return -1
			}
			inputPos++
		}
	}
	return inputPos
}

func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// findDelimiter finds the position of the delimiter character in input.
// If the delimiter is a whitespace character, matches any whitespace.
func findDelimiter(input string, delimiter byte) int {
	if isWhitespace(delimiter) {
		for i := 0; i < len(input); i++ {
			if isWhitespace(input[i]) {
				return i
			}
		}
		return -1
	}
	return strings.IndexByte(input, delimiter)
}

func indexOfNewline(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' || s[i] == '\r' {
			return i
		}
	}
	return -1
}

// makeUniqueColumnName appends a numeric suffix if the name has already been used.
func makeUniqueColumnName(base string, used map[string]int) string {
	used[base]++
	if used[base] == 1 {
		return base
	}
	return fmt.Sprintf("%s_%d", base, used[base])
}
