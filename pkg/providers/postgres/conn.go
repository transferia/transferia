//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"reflect"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// go-sumtype:decl DataTypesOption
type DataTypesOption interface {
	isDataTypeOption()
}

// The Go type for PostgreSQL types of which pgx driver is unaware.  Default is
// *pgx.GenericText. The implementation type must be a pointer (this is
// required by pgx). May be nil pointer.
type DefaultDataType struct{ pgtype.Value }

func (t *DefaultDataType) isDataTypeOption() {}

// Text representation of PostgreSQL type from pg_type catalog, opposed to OID: https://www.postgresql.org/docs/current/catalog-pg-type.html
type TypeFullName struct {
	Namespace string // typnamespace from pg_type
	Name      string // typname from pg_type
}

type TypeNameToOIDMap map[TypeFullName]pgtype.OID

func (m TypeNameToOIDMap) isDataTypeOption() {}

func MakeInitDataTypes(options ...DataTypesOption) func(ctx context.Context, conn *pgx.Conn) error {
	return func(ctx context.Context, conn *pgx.Conn) error {
		var defaultDataType pgtype.Value = ((*pgtype.GenericText)(nil))
		var nameToOID TypeNameToOIDMap
		for _, option := range options {
			switch opt := option.(type) {
			case *DefaultDataType:
				defaultDataType = opt.Value
			case TypeNameToOIDMap:
				nameToOID = opt
			}
		}

		if err := initBaseTypes(ctx, conn, defaultDataType); err != nil {
			return xerrors.Errorf("unable to initialize base types: %w", err)
		}

		if err := initConnInfoEnumArray(ctx, conn); err != nil {
			return xerrors.Errorf("unable to initialize enum arrays: %w", err)
		}

		if err := initConnInfoDomains(ctx, conn); err != nil {
			return xerrors.Errorf("unable to initialize domains: %w", err)
		}

		if err := initConnInfoArraysAndComposite(ctx, conn, nameToOID); err != nil {
			return xerrors.Errorf("unable to initialize complex types: %w", err)
		}

		return nil
	}
}

// Initializes the internal mapping between PostgreSQL type OID and the
// corresponding Go type for the "common" data types: base ('b'), enum ('e'),
// range ('r') and pseudo ('p'). Also sets the catch-all Go type
// `defaultDataType` for all the types which fall outside of that mapping.
// defaultDataType may be an empty instance of any concrete type implementing
// pgtype.Value. It is only used to get the type information about the
// implementation to create new instances of that type.
func initBaseTypes(ctx context.Context, conn *pgx.Conn, defaultDataType pgtype.Value) error {
	const (
		namedOIDQuery = `select t.oid,
	case when nsp.nspname in ('pg_catalog', 'public') then t.typname
		else nsp.nspname||'.'||t.typname
	end
from pg_type t
left join pg_type base_type on t.typelem=base_type.oid
left join pg_class base_cls ON base_type.typrelid = base_cls.oid
left join pg_namespace nsp on t.typnamespace=nsp.oid
where (
	  t.typtype in('b', 'p', 'r', 'e')
	  and (base_type.oid is null or base_type.typtype in('b', 'p', 'r'))
	)`
	)

	nameOIDs, err := connInfoFromRows(conn.Query(ctx, namedOIDQuery))
	if err != nil {
		return xerrors.Errorf("unable to process base types info: %w", err)
	}

	initializeDataTypes(conn.ConnInfo(), nameOIDs, defaultDataType)
	return nil
}

func connInfoFromRows(rows pgx.Rows, err error) (map[string]uint32, error) {
	if err != nil {
		return nil, xerrors.Errorf("unable to select types info: %w", err)
	}
	defer rows.Close()

	nameOIDs := make(map[string]uint32, 256)
	for rows.Next() {
		var oid pgtype.OID
		var name pgtype.Text
		if err = rows.Scan(&oid, &name); err != nil {
			return nil, xerrors.Errorf("unable to scan type info row: %w", err)
		}

		nameOIDs[name.String] = uint32(oid)
	}

	if err = rows.Err(); err != nil {
		return nil, xerrors.Errorf("unable to read type info rows: %w", err)
	}

	return nameOIDs, nil
}

// initConnInfoEnumArray introspects for arrays of enums and registers a data type for them.
func initConnInfoEnumArray(ctx context.Context, conn *pgx.Conn) error {
	nameOIDs := make(map[string]pgtype.OID, 16)
	rows, err := conn.Query(ctx, `select t.oid, t.typname
from pg_type t
  join pg_type base_type on t.typelem=base_type.oid
where t.typtype = 'b'
  and base_type.typtype = 'e'`)
	if err != nil {
		return xerrors.Errorf("unable to select enum array types info: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var oid pgtype.OID
		var name pgtype.Text
		if err := rows.Scan(&oid, &name); err != nil {
			return xerrors.Errorf("unable to scan enum array type info row: %w", err)
		}

		nameOIDs[name.String] = oid
	}

	if rows.Err() != nil {
		return xerrors.Errorf("unable to read enum array type info rows: %w", rows.Err())
	}

	connInfo := conn.ConnInfo()
	for name, oid := range nameOIDs {
		connInfo.RegisterDataType(pgtype.DataType{
			Value: &pgtype.EnumArray{},
			Name:  name,
			OID:   uint32(oid),
		})
	}

	return nil
}

// initConnInfoDomains introspects for domains and registers a data type for them.
func initConnInfoDomains(ctx context.Context, conn *pgx.Conn) error {
	type domain struct {
		oid     pgtype.OID
		name    pgtype.Text
		baseOID pgtype.OID
	}

	domains := make([]*domain, 0, 16)

	rows, err := conn.Query(ctx, `select t.oid, t.typname, t.typbasetype
from pg_type t
  join pg_type base_type on t.typbasetype=base_type.oid
where t.typtype = 'd'
  and base_type.typtype = 'b'`)
	if err != nil {
		return xerrors.Errorf("unable to select domain types info: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var d domain
		if err := rows.Scan(&d.oid, &d.name, &d.baseOID); err != nil {
			return xerrors.Errorf("unable to scan domain type info row: %w", err)
		}

		domains = append(domains, &d)
	}

	if rows.Err() != nil {
		return xerrors.Errorf("unable to read domain type info rows: %w", rows.Err())
	}

	connInfo := conn.ConnInfo()
	for _, d := range domains {
		baseDataType, ok := connInfo.DataTypeForOID(uint32(d.baseOID))
		if ok {
			connInfo.RegisterDataType(pgtype.DataType{
				Value: reflect.New(reflect.ValueOf(baseDataType.Value).Elem().Type()).Interface().(pgtype.Value),
				Name:  d.name.String,
				OID:   uint32(d.oid),
			})
		}
	}

	return nil
}

func initConnInfoArraysAndComposite(ctx context.Context, conn *pgx.Conn, nameToOID TypeNameToOIDMap) error {
	complexTypes, err := selectComplexTypes(ctx, conn)
	if err != nil {
		return err
	}

	sortedComplexTypes := sortComplexTypes(complexTypes)

	connInfo := conn.ConnInfo()
	for _, typ := range sortedComplexTypes {
		value, err := typ.NewValue(connInfo, nameToOID)
		if err != nil {
			if typ.FullName().Namespace == "pg_catalog" && typ.FullName().Name == "_pg_statistic" {
				continue
			}
			logger.Log.Warnf("unable to initialize type %s.%s (OID %d): %s", typ.FullName().Namespace, typ.FullName().Name, typ.OID(), err.Error())
			continue
		}
		connInfo.RegisterDataType(pgtype.DataType{Value: value, Name: typ.FullName().Name, OID: uint32(typ.OID())})
	}

	return nil
}
