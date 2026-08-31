package changeitem

import (
	"strings"
)

type TableID struct {
	Namespace string // Schema for PostgreSQL, database name for MySQL and MongoDB; empty string for YT/YDB
	Name      string // Table name for relational databases, collection name for MongoDB. For YDB - full path
}

// Fqtn returns a "fully qualified" table name / FQTN.
// FQTN is a string with special properties - see definition of this function to figure them out.
func (t TableID) Fqtn() string {
	parts := make([]string, 0, 2)
	if len(t.Namespace) > 0 {
		parts = append(parts, doubleQuoteAndEscape(t.Namespace))
	}
	if t.Name == "*" {
		parts = append(parts, t.Name)
	} else {
		parts = append(parts, doubleQuoteAndEscape(t.Name))
	}
	return strings.Join(parts, ".")
}

func doubleQuoteAndEscape(str string) string {
	return strings.Join([]string{"\"", strings.ReplaceAll(str, "\"", "\"\""), "\""}, "")
}

func (t TableID) Less(other TableID) int {
	if t.Namespace < other.Namespace {
		return -1
	}
	if t.Namespace > other.Namespace {
		return 1
	}
	return strings.Compare(t.Name, other.Name)
}

func (t TableID) String() string {
	return t.Fqtn()
}

func (t TableID) Equals(other TableID) bool {
	return t.Namespace == other.Namespace && t.Name == other.Name
}

// Includes returns true if the given ID identifies a subset of tables identified by the current ID.
func (t TableID) Includes(sub TableID) bool {
	if t.Equals(sub) {
		return true
	}
	if len(t.Namespace) == 0 {
		if t.Name == "*" {
			return true
		}
		return t.Name == sub.Name
	}
	if t.Namespace != sub.Namespace {
		return false
	}
	if t.Name == "*" {
		return true
	}
	return t.Name == sub.Name
}

func (t *TableID) IsSystemTable() bool {
	return IsSystemTable(t.Name)
}

func NewTableID(namespace string, name string) *TableID {
	return &TableID{Namespace: namespace, Name: name}
}
