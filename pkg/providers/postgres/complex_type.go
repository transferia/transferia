//go:build !disable_postgres_provider

package postgres

import (
	"context"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// complexType is a type which includes elements of other types. It is an
// abstraction over array types (which contain elements of a single type) and
// composite types (which may include arbitrary number of child types).
type complexType interface {
	OID() pgtype.OID

	// Returns full name of the complex type (schema + name). Used for
	// reporting errors in log and when registering a type within a PostgreSQL
	// driver connection (the driver internally maintains a name->OID map for
	// all types).
	FullName() TypeFullName

	// Returns OIDs of "child" types - element type for array, and field types
	// for composite type. This is used to figure out the proper order of
	// registering types within the driver: those types which do not depend on
	// anything will be registered first, and those which depend on everything
	// will be registered last.
	// Note that in PostgreSQL types may never refer themselves, nor form
	// loops.
	DependsOn() []pgtype.OID

	// Creates a new value of the complex type for registering it in the
	// driver.
	//
	// nameToOID is used to map the child types into the proper OIDs. When a
	// value of a complex type is selected in one database and then inserted
	// into another **in the binary format**, OIDs of the child types appear on
	// the wire. Types in different databases may have the same name, but have
	// different OIDs. In order to put the proper OIDs into the binary
	// serialization of a complex type, we try to map the type name to the OID
	// of that type in the receiving database.
	//
	// Note that if the original type name does not map to any OID in the
	// receiving database, we simply put the original type's OID on the wire.
	// This is OK, since the receiving server PostgreSQL actually checks if the
	// child type's OID on the wire matches the child type's OID in its pg_type
	// entry, and in the worst case we would get an SQL error when trying to
	// insert a value with unsuccessfully mapped OID.
	NewValue(connInfo *pgtype.ConnInfo, nameToOID TypeNameToOIDMap) (pgtype.Value, error)
}

func selectComplexTypes(ctx context.Context, conn *pgx.Conn) (map[pgtype.OID]complexType, error) {
	complexTypes := make(map[pgtype.OID]complexType)

	if err := selectCompositeTypes(ctx, conn, complexTypes); err != nil {
		return nil, xerrors.Errorf("unable to get composite types: %w", err)
	}

	if err := selectArrayTypes(ctx, conn, complexTypes); err != nil {
		return nil, xerrors.Errorf("unable to get array types: %w", err)
	}

	return complexTypes, nil
}

// Orders complex types so that i-th type never references i+n-th type through
// its attributes. That is, topologically sorts the complex types graph.
func sortComplexTypes(complexTypes map[pgtype.OID]complexType) (sortedTypes []complexType) {
	processedNodes := make(map[pgtype.OID]struct{})
	var dfs func(typ complexType)
	dfs = func(typ complexType) {
		if _, ok := processedNodes[typ.OID()]; ok {
			return
		}
		processedNodes[typ.OID()] = struct{}{}

		for _, oid := range typ.DependsOn() {
			childType, ok := complexTypes[oid]
			if !ok {
				continue // child type is not complex
			}
			dfs(childType)
		}

		sortedTypes = append(sortedTypes, typ)
	}

	for _, typ := range complexTypes {
		dfs(typ)
	}

	return sortedTypes
}
