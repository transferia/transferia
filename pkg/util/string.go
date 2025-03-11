package util

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

const (
	DefaultSampleLen = 1000
)

func Prefix(s string, prefixLen int) string {
	if len(s) > prefixLen {
		return string([]rune(s)[:prefixLen])
	}
	return s
}

func TailSample(s string, maxSampleLen int) string {
	if len(s) > maxSampleLen {
		return s[len(s)-maxSampleLen:] + fmt.Sprintf(" (%d characters more)", len(s)-maxSampleLen)
	} else {
		return s
	}
}

func Sample(s string, maxSampleLen int) string {
	if len(s) > maxSampleLen {
		return s[:maxSampleLen] + fmt.Sprintf(" (%d characters more)", len(s)-maxSampleLen)
	} else {
		return s
	}
}

func DefaultSample(s string) string {
	return Sample(s, DefaultSampleLen)
}

func SampleBytes(data []byte, maxSampleLen int) []byte {
	if len(data) > maxSampleLen {
		result := make([]byte, 0, maxSampleLen+100)
		result = append(result, data[:maxSampleLen]...)
		result = append(result, fmt.Sprintf(" (%d characters more)", len(data)-maxSampleLen)...)
		return result
	} else {
		return data
	}
}

func SampleHex(data []byte, maxSampleLen int) string {
	var nCharsTruncated int
	if len(data) > maxSampleLen {
		data = data[:maxSampleLen]
		nCharsTruncated = len(data) - maxSampleLen
	}

	result := hex.EncodeToString(data)
	if nCharsTruncated > 0 {
		result += fmt.Sprintf(" (%d bytes more)", nCharsTruncated)
	}
	return result
}

func ensureNoSpacesOnTheEdgesImpl(m map[string]json.RawMessage, structName string) error {
	for fieldName, v := range m {
		if len(v) > 0 {
			if v[0] == '"' && len(v) > 2 {
				valQuoted := string(v)
				valStr := valQuoted[1 : len(valQuoted)-1]
				if len(valStr) == 1 && valStr == " " {
					// deliberate space char as single value
					return nil
				}
				if firstRune, _ := utf8.DecodeRuneInString(valStr); unicode.IsSpace(firstRune) {
					return xerrors.New(fmt.Sprintf("Value of field %s.%s starts with a whitespace character. Value: \"%s\"", structName, fieldName, valStr))
				}
				if lastRune, _ := utf8.DecodeLastRuneInString(valStr); unicode.IsSpace(lastRune) {
					return xerrors.New(fmt.Sprintf("Value of field %s.%s ends with a whitespace character. Value: \"%s\"", structName, fieldName, valStr))
				}
			}

			if v[0] == '{' {
				var currVal map[string]json.RawMessage
				_ = json.Unmarshal(v, &currVal)
				err := ensureNoSpacesOnTheEdgesImpl(currVal, fmt.Sprintf("%s.%s", structName, fieldName))
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func EnsureNoStringValsOnTheEdges(s string) error {
	var v map[string]json.RawMessage
	_ = json.Unmarshal([]byte(s), &v)
	return ensureNoSpacesOnTheEdgesImpl(v, "")
}

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// IsASCIIPrintable - determines if this is string (safe to printing) or binary buffer
// taken from: https://stackoverflow.com/a/24677132
func IsASCIIPrintable(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}
