//go:build !disable_clickhouse_provider

package format

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCSVValidatorValidCase(t *testing.T) {
	input := `1,"\",3
          "test",123,""""
,,
"new line","\n",654321
",","\r",";"


		"without",new,line`
	inputReader := strings.NewReader(input)
	resultWriter := bytes.NewBuffer(nil)

	buffer := bytes.NewBuffer(nil)
	teeReader := io.TeeReader(inputReader, buffer)
	validator := NewCsvValidator(teeReader, 3)
	for i := 1; i <= 6; i++ {
		num, err := validator.ReadAndValidate()
		require.NoError(t, err, "line number %d", i)
		_, err = io.CopyN(resultWriter, buffer, num)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(input, resultWriter.String()), "result must be a prefix of input")
	}
	_, err := validator.ReadAndValidate()
	require.ErrorIs(t, err, io.EOF)
}

func TestCSVValidatorInvalidCases(t *testing.T) {
	testCases := []string{
		"string without new line",
		"one value\n",
		`"two values",` + "\n",
		`"wrong quotes escaping",""",some `,
		`"wrong quotes escaping 2 ",""""","some`,
		`four fields,"",some,`,
		`"invalid csv"","",test` + "\n",
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("testcase %d", i), func(t *testing.T) {
			inputReader := strings.NewReader(testCase)
			buffer := bytes.NewBuffer(nil)
			teeReader := io.TeeReader(inputReader, buffer)
			validator := NewCsvValidator(teeReader, 3)
			n, err := validator.ReadAndValidate()
			fmt.Println(n, err)
			require.Error(t, err)
		})
	}
}
