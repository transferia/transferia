package oraclerecipe

import (
	"context"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	oracle "github.com/transferia/transferia/pkg/providers/oracle"
	"github.com/transferia/transferia/pkg/providers/oracle/common"
)

// ExecSQL executes SQL statements against the Oracle instance.
//
// Volume mounting via testcontainers is not used for init SQL because the test binary's
// working directory in Arcadia is inside the build root, not the source tree — so relative
// paths to dump files cannot be resolved at container startup time. Instead, tests embed
// their init SQL with //go:embed and call ExecSQL after PrepareContainer.
func ExecSQL(ctx context.Context, src *oracle.OracleSource, sql string) error {
	db, err := common.CreateConnection(src)
	if err != nil {
		return xerrors.Errorf("create connection: %w", err)
	}
	defer db.Close()

	for _, stmt := range splitOracleStatements(sql) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return xerrors.Errorf("exec %q: %w", stmt, err)
		}
	}
	return nil
}

// splitOracleStatements splits a SQL script into individual executable statements.
// PL/SQL blocks (BEGIN...END;) are kept intact and passed with their internal semicolons.
// CREATE OR REPLACE FUNCTION/PROCEDURE/PACKAGE/TRIGGER statements are also kept intact,
// including their declaration sections (which appear before the BEGIN keyword).
// Regular DDL/DML statements are split on ";" and yielded without the trailing semicolon.
func splitOracleStatements(sql string) []string {
	var stmts []string
	var buf strings.Builder
	depth := 0
	inPLSQLDDL := false // true when inside a CREATE [OR REPLACE] FUNCTION/PROCEDURE/etc.

	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		upper := strings.ToUpper(trimmed)

		if depth == 0 && !inPLSQLDDL {
			for _, prefix := range []string{
				"CREATE OR REPLACE FUNCTION",
				"CREATE OR REPLACE PROCEDURE",
				"CREATE OR REPLACE PACKAGE",
				"CREATE OR REPLACE TRIGGER",
				"CREATE FUNCTION",
				"CREATE PROCEDURE",
				"CREATE PACKAGE",
				"CREATE TRIGGER",
			} {
				if strings.HasPrefix(upper, prefix) {
					inPLSQLDDL = true
					break
				}
			}
		}

		if upper == "BEGIN" || upper == "DECLARE" {
			depth++
		}

		buf.WriteString(line)
		buf.WriteByte('\n')

		if depth > 0 {
			// Inside a PL/SQL block: flush when the outermost END; is reached.
			if upper == "END;" {
				depth--
				if depth == 0 {
					if s := strings.TrimSpace(buf.String()); s != "" {
						stmts = append(stmts, s)
					}
					buf.Reset()
					inPLSQLDDL = false
				}
			}
		} else if !inPLSQLDDL && !strings.HasPrefix(trimmed, "--") {
			// Regular DML/DDL: split the accumulated buffer into statements.
			// Process line-by-line so that ";" inside "-- comment" lines is never
			// treated as a statement terminator, even when the comment sits in the
			// buffer before the current (non-comment) line that triggered the flush.
			content := buf.String()
			buf.Reset()
			var stmtBuf strings.Builder
			for _, contentLine := range strings.Split(content, "\n") {
				if strings.HasPrefix(strings.TrimSpace(contentLine), "--") {
					stmtBuf.WriteString(contentLine)
					stmtBuf.WriteByte('\n')
					continue
				}
				parts := strings.Split(contentLine, ";")
				for i, part := range parts {
					stmtBuf.WriteString(part)
					if i < len(parts)-1 {
						if s := strings.TrimSpace(stmtBuf.String()); s != "" {
							stmts = append(stmts, s)
						}
						stmtBuf.Reset()
					} else {
						stmtBuf.WriteByte('\n')
					}
				}
			}
			if s := strings.TrimSpace(stmtBuf.String()); s != "" {
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
		}
		// If inPLSQLDDL && depth == 0: we're in the declaration section before BEGIN — keep buffering.
	}

	if s := strings.TrimSpace(buf.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}
