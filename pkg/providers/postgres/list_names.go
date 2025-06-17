//go:build !disable_postgres_provider

package postgres

import "strings"

func ListWithCommaSingleQuoted(values []string) string {
	tS := make([]string, len(values))
	for i := range values {
		tS[i] = "'" + strings.ReplaceAll(values[i], "'", "''") + "'"
	}
	return strings.Join(tS, ", ")
}
