package log_miner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEscapedQuote(t *testing.T) {
	nextIndex := 57
	token := getToken([]rune(`insert into "PRODUCER"."EMP2"("EMPNO","ENAME") values (8,'a''b')`), &nextIndex)
	require.Equal(t, "'a'b'", token)
}

func TestTwoEscapedQuotes(t *testing.T) {
	nextIndex := 57
	token := getToken([]rune(`insert into "PRODUCER"."EMP2"("EMPNO","ENAME") values (8,'a''''b')`), &nextIndex)
	require.Equal(t, "'a''b'", token)
}

func TestEscapedQuoteNearRightBorder(t *testing.T) {
	nextIndex := 57
	token := getToken([]rune(`insert into "PRODUCER"."EMP2"("EMPNO","ENAME") values (8,'a'`), &nextIndex)
	require.Equal(t, "'a'", token)
}
