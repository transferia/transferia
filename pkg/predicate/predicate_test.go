package predicate

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
)

func TestColumnMatch(t *testing.T) {
	for i, tc := range []struct {
		predicate, col string
		val            string
		ops            int
	}{
		{
			predicate: "(\"version\" > '1') AND (NOT (\"version\" > '3'))",
			col:       "file_name",
			ops:       2,
		},
		{
			predicate: "(\"version\" > '1') AND (NOT (\"version\" > '3'))",
			col:       "version",
			ops:       2,
		},
		{
			predicate: "(\"version\" > '2023-04-11') AND (NOT (\"version\" > '2023-04-13')) AND (\"file_name\" = 'asdasd')",
			col:       "file_name",
			ops:       2,
		},
		{
			predicate: "(\"version\" > '2023-04-11') AND (NOT (\"version\" > '2023-04-13')) and (\"file_name\" < 'asdasd')",
			col:       "file_name",
			ops:       2,
		},
		{
			predicate: "(\"version\" > '2023-04-11') AND (\"file_name\" < 'asdasd')",
			col:       "file_name",
			ops:       1,
		},
		{
			predicate: "(\"file_name\" < 'asdasd') AND (\"version\" > '2023-04-11')",
			col:       "file_name",
			ops:       1,
		},
	} {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			p := NewParser(strings.NewReader(tc.predicate))
			expr, err := p.Parse()
			require.NoError(t, err)
			logger.Log.Infof("%s: %s", tc.predicate, expr.String())
			a := traverseOperands(expr, "version", 0, nil, false)
			require.NotNil(t, a)
			require.Len(t, a, tc.ops)
		})
	}
}
