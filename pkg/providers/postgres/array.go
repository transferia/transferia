//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	arrayTypesQuery = `
		SELECT DISTINCT
			pgt.oid        AS  oid,
			pgnsp.nspname  AS  namespace,
			pgt.typname    AS  name,
			pget.oid       AS  element_oid,
			pgensp.nspname AS  element_type_namespace,
			pget.typname   AS  element_type_name
		FROM pg_type pgt
			JOIN pg_type                 pget   ON pgt.typelem       = pget.oid
			JOIN pg_catalog.pg_attribute pgatt  ON pgt.oid           = pgatt.atttypid
			JOIN pg_namespace            pgnsp  ON pgt.typnamespace  = pgnsp.oid
			JOIN pg_namespace            pgensp ON pget.typnamespace = pgensp.oid
		WHERE 1=1
			AND pgt.typelem <> 0::oid
			AND pgt.typlen   = -1
	`
)

type array struct {
	oid                  pgtype.OID
	namespace            string
	name                 string
	elementTypeNamespace string
	elementTypeName      string
	elementTypeOID       pgtype.OID
	logger               log.Logger
}

func (a *array) OID() pgtype.OID {
	return a.oid
}

func (a *array) FullName() TypeFullName {
	return TypeFullName{Namespace: a.namespace, Name: a.name}
}

func (a *array) DependsOn() []pgtype.OID {
	return []pgtype.OID{a.elementTypeOID}
}

func (a *array) NewValue(connInfo *pgtype.ConnInfo, nameToOID TypeNameToOIDMap) (pgtype.Value, error) {
	elementDataType, ok := connInfo.DataTypeForOID(uint32(a.elementTypeOID))
	if !ok {
		return nil, xerrors.Errorf("unknown data type for OID %d (element of array %s)", a.elementTypeOID, a.name)
	}

	elementTypeName := TypeFullName{Namespace: a.elementTypeNamespace, Name: a.elementTypeName}
	elementTypeOID, ok := nameToOID[elementTypeName]
	if !ok {
		a.logger.Debug(
			"cannot map array element type to OID",
			log.String("array_type_name", fmt.Sprintf("%s.%s", a.namespace, a.name)),
			log.Reflect("element_type_name", fmt.Sprintf("%s.%s", elementTypeName.Namespace, elementTypeName.Name)),
		)
		elementTypeOID = a.elementTypeOID
	} else {
		a.logger.Debug(
			"mapped array element type to OID",
			log.String("array_type_name", fmt.Sprintf("%s.%s", a.namespace, a.name)),
			log.Reflect("element_type_name", fmt.Sprintf("%s.%s", elementTypeName.Namespace, elementTypeName.Name)),
			log.Int32("element_type_mapped_oid", int32(elementTypeOID)),
		)
	}

	return NewGenericArray(a.logger, a.name, elementTypeOID, func() (pgtype.Value, error) {
		return pgtype.NewValue(elementDataType.Value), nil
	})
}

func selectArrayTypes(ctx context.Context, conn *pgx.Conn, complexTypes map[pgtype.OID]complexType) error {
	rows, err := conn.Query(ctx, arrayTypesQuery)
	if err != nil {
		return xerrors.Errorf("select: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var arr array
		if err := rows.Scan(&arr.oid, &arr.namespace, &arr.name, &arr.elementTypeOID, &arr.elementTypeNamespace, &arr.elementTypeName); err != nil {
			return xerrors.Errorf("scan: %w", err)
		}
		arr.logger = log.With(
			logger.Log,
			log.UInt32("type_oid", uint32(arr.oid)),
			log.String("type_name", arr.name),
			log.UInt32("element_oid", uint32(arr.elementTypeOID)),
			log.String("element_name", arr.elementTypeName),
		)
		complexTypes[arr.oid] = &arr
	}
	return rows.Err()
}
