package csv

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"
)

func TestReadAll(t *testing.T) {
	t.Run("simple example", func(t *testing.T) {
		content := bytes.NewBufferString(`1, 2, 3
		a,b,c
		7,8,9` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)

		result, err := csvReader.ReadAll()
		require.NoError(t, err)
		// we expect 3 lines each with 3 elements

		require.Len(t, result, 3)
		require.Len(t, result[1], 3)
	})

	t.Run("newline is parsed as one single element", func(t *testing.T) {
		content := bytes.NewBufferString(`1, 2,   3," 4
		4    ", 5` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.NewlinesInValue = true

		result, err := csvReader.ReadAll()
		require.NoError(t, err)
		require.Equal(t, " 4\n\t\t4    ", result[0][3])
		require.Len(t, result, 1)
	})

	t.Run("quoted newline in data with false setting NewlinesInValue", func(t *testing.T) {
		content := bytes.NewBufferString("123123,\"2000-01-01\",\"\nmysuperdata\"")

		csvReader := NewReader(bufio.NewReader(content))
		csvReader.NewlinesInValue = false

		_, err := csvReader.ReadAll()
		require.Error(t, err)
	})

	t.Run("quoting is disallowed", func(t *testing.T) {
		content := bytes.NewBufferString(`a, b, "c", d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.QuoteChar = 0

		_, err := csvReader.ReadAll()

		require.ErrorIs(t, err, errQuotingDisabled)
	})

	t.Run("using uninitialized / invalid delimiter", func(t *testing.T) {
		content := bytes.NewBufferString(`a, b, "c", d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.Delimiter = 0

		_, err := csvReader.ReadAll()

		require.ErrorIs(t, err, errInvalidDelimiter)
	})

	t.Run("escape char outside of quotes treated as normal char", func(t *testing.T) {
		content := bytes.NewBufferString(`a, \, "c \" e , f"` + "\n")
		reader := bufio.NewReader(content)
		csvReader := NewReader(reader)

		res, err := csvReader.ReadAll()
		require.NoError(t, err)
		require.Equal(t, "\\", res[0][1])
		require.Equal(t, `c \" e , f`, res[0][2])
		require.Len(t, res, 1)
	})

	t.Run("no escape char configured", func(t *testing.T) {
		content := bytes.NewBufferString(`a, \, "c \" e , f"` + "\n")

		reader := bufio.NewReader(content)
		csvReader := NewReader(reader)

		var escape rune
		csvReader.EscapeChar = escape

		res, err := csvReader.ReadAll()
		require.NoError(t, err)
		require.Equal(t, "\\", res[0][1])
		require.Equal(t, `"c \" e`, res[0][2])
		require.Equal(t, `f"`, res[0][3])

		require.Len(t, res, 1)
	})

	t.Run("double quoting is disallowed", func(t *testing.T) {
		content := bytes.NewBufferString(`a, b, "the main ""test"" is this", d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.DoubleQuote = false

		_, err := csvReader.ReadAll()
		require.Error(t, err)

		require.ErrorIs(t, err, errDoubleQuotesDisabled)
	})

	t.Run("double quoting is allowed", func(t *testing.T) {
		content := bytes.NewBufferString(`a, b, "the main ""test"" is this", d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.DoubleQuote = true

		result, err := csvReader.ReadAll()
		require.NoError(t, err)

		require.Equal(t, [][]string{{"a", "b", "the main \"test\" is this", "d"}}, result)
	})

	t.Run("using different delimiter", func(t *testing.T) {
		content := bytes.NewBufferString(`a; b; "c"; d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.Delimiter = ';'

		result, err := csvReader.ReadAll()
		require.NoError(t, err)
		require.Equal(t, [][]string{{"a", "b", "c", "d"}}, result)
	})

	t.Run("using different quotes char", func(t *testing.T) {
		content := bytes.NewBufferString(`a, (b(, (c(, d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.QuoteChar = '('

		result, err := csvReader.ReadAll()
		require.NoError(t, err)
		require.Equal(t, [][]string{{"a", "b", "c", "d"}}, result)
	})

	t.Run("delimiter in quotes is not used as separator", func(t *testing.T) {
		content := bytes.NewBufferString(`1, "2", "3 , 3", 4` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)

		result, err := csvReader.ReadAll()
		require.NoError(t, err)
		//require.Len(t, result[0], 4)
		require.Equal(t, [][]string{{"1", "2", "3 , 3", "4"}}, result)
	})

	t.Run("double quotes are converted to single quotes", func(t *testing.T) {
		content := bytes.NewBufferString(`1, ""2"", 3` + "\n")

		reader := bufio.NewReader(content)
		csvReader := NewReader(reader)

		result, err := csvReader.ReadAll()
		require.NoError(t, err)
		require.Len(t, result[0], 3)
		require.Equal(t, [][]string{{"1", "\"2\"", "3"}}, result)
	})

	t.Run("different encoding for string is still valid", func(t *testing.T) {
		encoder := charmap.ISO8859_1.NewEncoder()

		isoEncodedString, err := encoder.String(`1, ""äääää"", 3` + "\n")
		require.NoError(t, err)

		content := bytes.NewBufferString(isoEncodedString)

		reader := bufio.NewReader(content)
		csvReader := NewReader(reader)

		// try without encoding, see that string is broken
		result, err := csvReader.ReadAll()
		require.NoError(t, err)
		require.Equal(t, [][]string{{"1", "\"\xe4\xe4\xe4\xe4\xe4\"", "3"}}, result)

		// if we set the encoding, its decoded correctly
		content2 := bytes.NewBufferString(isoEncodedString)
		reader2 := bufio.NewReader(content2)
		csvReader2 := NewReader(reader2)
		csvReader2.Encoding = charmap.ISO8859_1.String()

		result, err = csvReader2.ReadAll()
		require.NoError(t, err)
		require.Equal(t, [][]string{{"1", "\"äääää\"", "3"}}, result)
	})

	t.Run("quoted values appears with no quotes", func(t *testing.T) {
		content := bytes.NewBufferString(`1, "check check", 3` + "\n")

		reader := bufio.NewReader(content)
		csvReader := NewReader(reader)

		result, err := csvReader.ReadAll()
		require.NoError(t, err)
		require.Equal(t, [][]string{{"1", "check check", "3"}}, result)
	})

	t.Run("quoted values appears with no quotes 2", func(t *testing.T) {
		content := bytes.NewBufferString(`"5.155.155.155"|"-"|"-"|"2025-08-08 08:15:28"|"GET /aaa?ip_aaa=127.0.0.1&template_path=|ba+ff342.txt|cat HTTP/1.1"|"404"|"189"|"-"|"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6.1 Safari/605.1.15"|"656"|"m9-up-gc46"|"http"|"shield_media.tinkoffjournal.ru"|"0.023"|"0.023"|"674"|"-"|"m9"|"MISS"|"189"|"213.180.193.247:443"|"875"|"6377"|"-"|"-"|"RU"|"Moscow"|"shield_no"|"92.223.123.30"|"10080"|"404"|"-"|"0.000"|"0.023"|"127.0.0.1"|"210756"|"4316125312"|"1"|"asdasd132123asd"|"https"|"123123123123"|"-"|"-"|"-"|"text/html"|"61"|"HTTP/1.1"|"0"|"MOW"` + "\n")
		reader := bufio.NewReader(content)
		csvReader := NewReader(reader)
		csvReader.Delimiter = '|'

		result, err := csvReader.ReadAll()
		require.NoError(t, err)
		require.Equal(t, [][]string{{"5.155.155.155", "-", "-", "2025-08-08 08:15:28", "GET /aaa?ip_aaa=127.0.0.1&template_path=|ba+ff342.txt|cat HTTP/1.1", "404", "189", "-", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6.1 Safari/605.1.15", "656", "m9-up-gc46", "http", "shield_media.tinkoffjournal.ru", "0.023", "0.023", "674", "-", "m9", "MISS", "189", "213.180.193.247:443", "875", "6377", "-", "-", "RU", "Moscow", "shield_no", "92.223.123.30", "10080", "404", "-", "0.000", "0.023", "127.0.0.1", "210756", "4316125312", "1", "asdasd132123asd", "https", "123123123123", "-", "-", "-", "text/html", "61", "HTTP/1.1", "0", "MOW"}}, result)
	})
}

func TestGetEncodingDecoder(t *testing.T) {
	r := NewReader(nil)
	for _, enc := range charmap.All {
		if encStringer, ok := enc.(fmt.Stringer); ok {
			encodingName := encStringer.String()
			r.Encoding = encodingName
			require.NotNil(t, r.getEncodingDecoder())
			require.Equal(t, enc.NewDecoder(), r.getEncodingDecoder())
		} else {
			require.Nil(t, r.getEncodingDecoder())
		}
	}
}
