//go:build !disable_postgres_provider

package postgres

import (
	"context"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type tableInformationSchema struct {
	Table         abstract.TableDescription
	IsView        bool
	IsPartitioned bool
	HasSubclass   bool
	IsInherited   bool
	ParentTable   string
}

func newTableInformationSchema(ctx context.Context, conn pgxtype.Querier, table abstract.TableDescription) (*tableInformationSchema, error) {
	var relkind string
	var hasSubClass bool
	var isInherited bool
	var parentTable string

	if err := conn.QueryRow(
		ctx,
		`SELECT
                       c.relkind::text,
                       c.relhassubclass,
                       coalesce((SELECT inhseqno = 1 FROM pg_catalog.pg_inherits WHERE  inhrelid = c.oid), false) as is_inherited,
                       coalesce((SELECT inhparent::regclass::text FROM pg_catalog.pg_inherits WHERE  inhrelid = c.oid), '') as parent_table
               FROM pg_class c
               INNER JOIN pg_namespace ns ON c.relnamespace = ns.oid
               WHERE
                       ns.nspname = $1 and c.relname = $2;`,
		table.Schema,
		table.Name,
	).Scan(&relkind, &hasSubClass, &isInherited, &parentTable); err != nil {
		return nil, xerrors.Errorf("failed to execute query: %w", err)
	}

	return &tableInformationSchema{
		Table:         table,
		IsView:        relkind == "v",
		IsPartitioned: relkind == "p",
		HasSubclass:   hasSubClass,
		IsInherited:   isInherited,
		ParentTable:   parentTable,
	}, nil
}
