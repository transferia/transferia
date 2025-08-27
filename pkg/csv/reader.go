package csv

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

// For now, it's by default custom csv (not original csv)
// Custom - bcs of default EscapeChar == '\\', so it's like an extension of format
// backslashes in csv generates apache spark, for example.
// Into original RFC there are nothing about backslashes
// 	   https://www.ietf.org/rfc/rfc4180.txt
// Will discuss in TM-7436 changing default EscapeChar to 0, or maybe add several constructors: NewCSVRFC4180, ...

type Reader struct {
	Delimiter       rune
	QuoteChar       rune
	EscapeChar      rune
	DoubleQuoteStr  string
	Encoding        string
	DoubleQuote     bool
	NewlinesInValue bool
	reader          *bufio.Reader
	offset          int64
	mu              sync.Mutex
}

func (r *Reader) ReadAll() ([][]string, error) {
	var lines [][]string
	for {
		line, err := r.ReadLine()
		if xerrors.Is(err, io.EOF) {
			return lines, nil
		}
		if err != nil {
			return nil, xerrors.Errorf("failed to read all lines: %w", err)
		}
		if line != nil {
			lines = append(lines, line)
		}
	}
}

// GetOffset returns the offset(amount of bytes read by the reader).
func (r *Reader) GetOffset() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.offset
}

func (r *Reader) increaseOffset(valToIncrease int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.offset += valToIncrease
	return r.offset
}

func (r *Reader) ReadLine() ([]string, error) {
	if !validDelimiter(r.Delimiter) {
		return nil, errInvalidDelimiter
	}

	// only handle differently if newlines allowed and quote char is not disallowed
	if r.NewlinesInValue && r.QuoteChar != 0 {
		entries, err := r.readMultiline()
		if err != nil {
			return nil, xerrors.Errorf("failed to read multiline line: %w", err)
		}
		return entries, nil
	} else {
		entries, err := r.readSingleLine()
		if err != nil {
			return nil, xerrors.Errorf("failed to read single line: %w", err)
		}
		return entries, nil
	}
}

func (r *Reader) readMultiline() ([]string, error) {
	// read until end of multiline entry
	var fullLine string

	for {
		partialLine, err := r.readAndDecodeLine()
		if err != nil {
			return nil, xerrors.Errorf("unable to read line from csv: %w", err)
		}
		if len(partialLine) <= 1 {
			if partialLine == "\r" || partialLine == "\n" || partialLine == "" {
				// skip empty line
				continue
			}
		}
		fullLine = fmt.Sprintf("%s%s", fullLine, partialLine)
		if r.checkCompleteQuotes(fullLine) {
			break
		}
	}

	entries, err := r.splitString(fullLine)
	if err != nil {
		return nil, xerrors.Errorf("failed to split csv line into entries: %w", err)
	}
	return entries, nil
}

func (r *Reader) readSingleLine() ([]string, error) {
	line, err := r.readAndDecodeLine()
	if err != nil {
		return nil, xerrors.Errorf("failed to read line from csv: %w", err)
	}

	if len(line) <= 1 {
		if line == "\r" || line == "\n" {
			return nil, nil
		}
	}

	entries, err := r.splitString(line)
	if err != nil {
		return nil, xerrors.Errorf("failed to split csv line into entries: %w", err)
	}
	return entries, nil
}

// readAndDecodeLine reads until \n from buffer and returns a string decoded based on configured Encoding
func (r *Reader) readAndDecodeLine() (string, error) {
	var err error
	read, err := r.reader.ReadBytes('\n')
	if err != nil {
		if err == io.EOF && len(read) > 0 {
			// drop last incomplete line
			return "", io.EOF
		} else {
			return "", xerrors.Errorf("failed to read line from csv: %w", err)
		}
	}
	r.increaseOffset(int64(len(read)))

	var line string

	decoder := r.getEncodingDecoder()
	if decoder == nil {
		// is utf8, simple conversion to string
		line = string(read)
	} else {
		converted, err := decoder.Bytes(read)
		if err != nil {
			return "", xerrors.Errorf("failed to convert to utf8 encoding from %s: %w", r.Encoding, err)
		}
		line = string(converted)
	}

	if r.QuoteChar == 0 && strings.Contains(string(line), string('"')) {
		return "", errQuotingDisabled
	}

	return line, nil
}

// checkCompleteQuotes checks that all quoted fields are complete (contain opening an closing quote).
func (r *Reader) checkCompleteQuotes(element string) bool {
	var prev rune
	var inQuotes bool

	for _, char := range element {
		if r.EscapeChar != 0 && r.EscapeChar == prev {
			// current char is escaped, ignore
			if inQuotes {
				prev = char
				continue
			}
		}

		if r.QuoteChar != 0 && char == r.QuoteChar {
			// switch
			inQuotes = !inQuotes
			prev = char
			continue
		}

		prev = char
	}

	// if element is not fully enclosed in quotes its not complete
	return !inQuotes
}

func (r *Reader) splitString(line string) ([]string, error) {
	var result []string
	var prev rune

	var lastDelimPosition int
	var prevDelimPosition int

	var inQuotes bool

	for index, char := range line {
		if r.EscapeChar != 0 && r.EscapeChar == prev {
			// current char is escaped, ignore it
			if inQuotes {
				prev = char
				continue
			}
		}

		if r.QuoteChar != 0 && char == r.QuoteChar {
			// switch
			inQuotes = !inQuotes
			prev = char
			continue
		}

		if char == r.Delimiter && !inQuotes {
			// found end of one element
			lastDelimPosition = index

			element := line[prevDelimPosition:lastDelimPosition]

			element, err := r.sanitizeElement(element)
			if err != nil {
				return nil, xerrors.Errorf("failed to sanitize element: %w", err)
			}

			result = append(result, element)
			prevDelimPosition = lastDelimPosition + 1
		}

		prev = char
	}

	lastElement, err := r.sanitizeElement(line[lastDelimPosition+1:])
	if err != nil {
		return nil, xerrors.Errorf("failed to sanitize element: %w", err)
	}

	result = append(result, lastElement)

	return result, nil
}

func (r *Reader) sanitizeElement(toSanitize string) (string, error) {
	// remove spaces
	element := strings.TrimSpace(toSanitize)

	if r.QuoteChar != 0 {
		var err error
		element, err = r.unquote(element)
		if err != nil {
			return "", xerrors.Errorf("failed to swap quotes: %w", err)
		}

		// replace double quotes, for example "" with single quote "
		element, err = r.swapToSingleQuotes(element)
		if err != nil {
			return "", xerrors.Errorf("failed to replace double quotes: %w", err)
		}
	}
	return element, nil
}

// swapQuotesChar swaps the used quote char at start and end of value (if present) with quotes.
func (r *Reader) unquote(element string) (string, error) {
	if len(element) == 0 {
		return "", nil
	}

	q := byte(r.QuoteChar)
	if len(element) == 1 && element[0] == q {
		return "", xerrors.Errorf("got element with value of only one quote. Check that data does not contain newlines")
	}
	if element[0] == q && element[len(element)-1] == q {
		return element[1 : len(element)-1], nil
	}
	return element, nil
}

// swapToSingleQuotes swaps possible double quotes with single quote in a field.
// Throws errDoubleQuotesDisabled if double quotes are present in the field even though functionality is disabled.
func (r *Reader) swapToSingleQuotes(element string) (string, error) {
	if !r.DoubleQuote {
		if strings.Contains(element, r.DoubleQuoteStr) {
			// double quote disabled, throw error
			return "", errDoubleQuotesDisabled
		} else {
			// string is fine as-is
			return element, nil
		}
	} else {
		res := strings.ReplaceAll(element, r.DoubleQuoteStr, string('"'))
		return res, nil
	}
}

func validDelimiter(r rune) bool {
	return r != 0 && r != '\r' && r != '\n' && utf8.ValidRune(r) && r != utf8.RuneError
}

func (r *Reader) getEncodingDecoder() *encoding.Decoder {
	// TODO: add other encodings ?
	switch r.Encoding {
	case charmap.ISO8859_1.String():
		return charmap.ISO8859_1.NewDecoder()
	case charmap.Macintosh.String():
		return charmap.Macintosh.NewDecoder()
	default:
		// utf8 encoding, no decoder needed
		return nil
	}
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		Delimiter:       ',',
		QuoteChar:       '"',
		DoubleQuoteStr:  `""`,
		EscapeChar:      '\\',
		Encoding:        "utf_8",
		DoubleQuote:     true,
		NewlinesInValue: false,
		reader:          bufio.NewReader(r),
		offset:          0,
		mu:              sync.Mutex{},
	}
}
