//go:build !disable_postgres_provider

package postgres

import (
	"bufio"
	_ "embed"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

var (
	//go:embed postgres_keywords.txt
	keywordsTxt string

	keywordsMap = makeKeywordsMap(keywordsTxt)
)

func makeKeywordsMap(keywordsTxt string) map[string]struct{} {
	scanner := bufio.NewScanner(strings.NewReader(keywordsTxt))
	kwMap := make(map[string]struct{})

	i := 1
	for scanner.Scan() {
		kw := scanner.Text()
		kwMap[kw] = struct{}{}
		i++
	}
	if scanner.Err() != nil {
		panic(xerrors.Errorf("Cannot scan line %d of postgres_keywords.txt: %w", i, scanner.Err()))
	}
	if len(kwMap) == 0 {
		panic(xerrors.New("Keyword map is empty"))
	}
	return kwMap
}

// Returns whether name is a keyword or not according to a well-known keywords list.
// See https://www.postgresql.org/docs/current/sql-keywords-appendix.html
func IsKeyword(name string) bool {
	_, isKw := keywordsMap[strings.ToUpper(name)]
	return isKw
}

// Sanitize sanitizes the given identifier so that it can be used safely in queries to PostgreSQL. Note that the input cannot contain multiple identifiers (e.g., `schema.table` is not a valid input).
func Sanitize(identifier string) string {
	return pgx.Identifier([]string{identifier}).Sanitize()
}
