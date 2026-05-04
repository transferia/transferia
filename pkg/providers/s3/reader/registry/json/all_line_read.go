package json

import (
	parsers_scanner "github.com/transferia/transferia/pkg/parsers/scanner"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/valyala/fastjson"
)

//nolint:descriptiveerrors
func readAllLines(content []byte, fileKey string) ([]string, uint64, reader_error.ReaderError) {
	currScanner := parsers_scanner.NewLineBreakScanner(content)
	scannedLines, err := currScanner.ScanAll()
	if err != nil {
		return nil, 0, reader_error.NewReaderErrorDataFile("json.readAllLines.scan", fileKey, err)
	}

	var lines []string

	bytesRead := uint64(0)
	for index, line := range scannedLines {
		if index == len(scannedLines)-1 {
			// check if last line is complete
			if err := fastjson.Validate(line); err != nil {
				break
			}
		}
		lines = append(lines, line)
		bytesRead += uint64(len(line)) + 1 // 1 - it's 'len("\n")'
	}
	return lines, bytesRead, nil
}

// In order to comply with the POSIX standard definition of line https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_206
func readAllMultilineLines(content []byte) ([]string, uint64) {
	if len(content) == 0 {
		return make([]string, 0), 0
	}

	var lines []string
	extractedLine := make([]rune, 0)
	foundStart := false
	countCurlyBrackets := 0
	bytesRead := uint64(0)
	inString := false
	escaped := false

	for _, char := range string(content) {
		if foundStart && countCurlyBrackets == 0 {
			lines = append(lines, string(extractedLine))
			bytesRead += uint64(len(string(extractedLine)) + 1) // 1 - it's 'len("\n")'

			foundStart = false
			extractedLine = make([]rune, 0)
			inString = false
			escaped = false
			continue
		}
		extractedLine = append(extractedLine, char)

		// Handle escape sequences
		if escaped {
			escaped = false
			continue
		}

		if char == '\\' {
			escaped = true
			continue
		}

		// Toggle string state on unescaped quotes
		if char == '"' {
			inString = !inString
			continue
		}

		// Only count brackets when not inside a string
		if !inString {
			if char == '{' {
				countCurlyBrackets++
				foundStart = true
				continue
			}

			if char == '}' {
				countCurlyBrackets--
			}
		}
	}
	if foundStart && countCurlyBrackets == 0 && content[len(content)-1] == '}' {
		lines = append(lines, string(extractedLine))
		bytesRead += uint64(len(string(extractedLine)))
	}
	return lines, bytesRead
}
