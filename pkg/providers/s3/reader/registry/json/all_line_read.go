package reader

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsers/scanner"
	"github.com/valyala/fastjson"
)

func readAllLines(content []byte) ([]string, int, error) {
	currScanner := scanner.NewLineBreakScanner(content)
	scannedLines, err := currScanner.ScanAll()
	if err != nil {
		return nil, 0, xerrors.Errorf("failed to split all read lines: %w", err)
	}

	var lines []string

	bytesRead := 0
	for index, line := range scannedLines {
		if index == len(scannedLines)-1 {
			// check if last line is complete
			if err := fastjson.Validate(line); err != nil {
				break
			}
		}
		lines = append(lines, line)
		bytesRead += (len(line) + len("\n"))
	}
	return lines, bytesRead, nil
}

// In order to comply with the POSIX standard definition of line https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_206
func readAllMultilineLines(content []byte) ([]string, int) {
	if len(content) == 0 {
		return make([]string, 0), 0
	}

	var lines []string
	extractedLine := make([]rune, 0)
	foundStart := false
	countCurlyBrackets := 0
	bytesRead := 0
	inString := false
	escaped := false

	for _, char := range string(content) {
		if foundStart && countCurlyBrackets == 0 {
			lines = append(lines, string(extractedLine))
			bytesRead += (len(string(extractedLine)) + len("\n"))

			foundStart = false
			extractedLine = []rune{}
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
		bytesRead += len(string(extractedLine))
	}
	return lines, bytesRead
}
