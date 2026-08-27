package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateFromLSNQueryQualifiesAndEscapesSchema(t *testing.T) {
	require.Equal(
		t,
		`select * from "custom""schema"."pg_create_logical_replication_slot_lsn"($1, 'wal2json', false, pg_lsn($2))`,
		createFromLSNQuery(`custom"schema`),
	)
}

func TestCreateFromLSNQueryDoesNotDependOnSearchPath(t *testing.T) {
	require.Equal(
		t,
		`select * from "public"."pg_create_logical_replication_slot_lsn"($1, 'wal2json', false, pg_lsn($2))`,
		createFromLSNQuery("public"),
	)
}
