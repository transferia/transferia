//go:build !disable_postgres_provider

package postgres

import (
	"fmt"
)

type PgObjectType string

const (
	Table            PgObjectType = "TABLE"
	TableAttach      PgObjectType = "TABLE_ATTACH"
	PrimaryKey       PgObjectType = "PRIMARY_KEY"
	View             PgObjectType = "VIEW"
	Sequence         PgObjectType = "SEQUENCE"
	SequenceSet      PgObjectType = "SEQUENCE_SET"
	SequenceOwnedBy  PgObjectType = "SEQUENCE_OWNED_BY"
	Rule             PgObjectType = "RULE"
	Type             PgObjectType = "TYPE"
	Constraint       PgObjectType = "CONSTRAINT"
	FkConstraint     PgObjectType = "FK_CONSTRAINT"
	Index            PgObjectType = "INDEX"
	IndexAttach      PgObjectType = "INDEX_ATTACH"
	Function         PgObjectType = "FUNCTION"
	Collation        PgObjectType = "COLLATION"
	Trigger          PgObjectType = "TRIGGER"
	Policy           PgObjectType = "POLICY"
	Cast             PgObjectType = "CAST"
	MaterializedView PgObjectType = "MATERIALIZED_VIEW"
)

func (pot PgObjectType) IsValid() error {
	switch pot {
	case Table, TableAttach, PrimaryKey, View, Sequence, SequenceSet, SequenceOwnedBy,
		Rule, Type, Constraint, FkConstraint, Index, IndexAttach,
		Function, Collation, Trigger, Policy, Cast, MaterializedView:
		return nil
	}
	return fmt.Errorf("invalid PgObjectType: %v", pot)
}
