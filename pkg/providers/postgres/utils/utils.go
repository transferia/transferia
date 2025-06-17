//go:build !disable_postgres_provider

package utils

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/set"
)

const (
	// PostgreSQL limitation: https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
	postgreSQLTableNameMaxLength = 130
)

// Purpose of function HandleHostAndHosts - guarantee smooth transition from 'host' to 'hosts'
// it allows to be filled both fields - 'host' & 'hosts' - can be useful on migration period.
// it saves order, but enforces uniqueness.
func HandleHostAndHosts(host string, hosts []string) []string {
	allHosts := yslices.Filter(append([]string{host}, hosts...), func(s string) bool { return s != "" })
	allHosts = set.New(allHosts...).SortedSliceFunc(func(a, b string) bool {
		return a < b
	})

	if len(allHosts) == 0 {
		return nil
	}
	return allHosts
}

func ValidatePGTables(tables []string) error {
	for _, table := range tables {
		if len(table) >= 2 && table[len(table)-2:] == ".*" {
			continue
		}

		if _, err := abstract.ParseTableID(table); err != nil {
			return xerrors.Errorf("can't parse include table name '%v': %w", table, err)
		}
		if len(table) > postgreSQLTableNameMaxLength {
			return xerrors.Errorf("length of include table name '%v' longer than maximum %v", table, postgreSQLTableNameMaxLength)
		}
	}
	return nil
}
