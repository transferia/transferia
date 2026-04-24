package oraclerecipe

import (
	"context"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	oracle "github.com/transferia/transferia/pkg/providers/oracle"
	"github.com/transferia/transferia/pkg/providers/oracle/common"
)

// ExecSQL executes semicolon-separated SQL statements against the Oracle instance.
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

	for _, stmt := range strings.Split(sql, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return xerrors.Errorf("exec %q: %w", stmt, err)
		}
	}
	return nil
}
