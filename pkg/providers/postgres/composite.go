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
	compositeTypesQuery = `
		SELECT
			t.oid,
			nsp.nspname,
			t.typname,
			a.attname,
			a.atttypid,
			ensp.nspname,
			et.typname
		FROM pg_type t
			JOIN pg_class     cls  ON t.typrelid      = cls.oid
			JOIN pg_attribute a    ON a.attrelid      = cls.oid
			JOIN pg_type      et   ON a.atttypid      = et.oid
			JOIN pg_namespace nsp  ON t.typnamespace  = nsp.oid
			JOIN pg_namespace ensp ON et.typnamespace = ensp.oid
		WHERE 1=1
			AND t.typtype   = 'c'
			AND cls.relkind = 'c'
		ORDER BY
			a.attrelid,
			a.attnum asc
	`
)

type composite struct {
	oid       pgtype.OID
	namespace string
	name      string
	fields    []compositeTypeField
}

type compositeTypeField struct {
	oid           pgtype.OID
	typeNamespace string
	typeName      string
	fieldName     string
}

func (c *composite) OID() pgtype.OID {
	return c.oid
}

func (c *composite) FullName() TypeFullName {
	return TypeFullName{Namespace: c.namespace, Name: c.name}
}

func (c *composite) DependsOn() []pgtype.OID {
	var fieldTypes []pgtype.OID
	for _, f := range c.fields {
		fieldTypes = append(fieldTypes, pgtype.OID(f.oid))
	}
	return fieldTypes
}

func (c *composite) NewValue(connInfo *pgtype.ConnInfo, nameToOID TypeNameToOIDMap) (pgtype.Value, error) {
	pgtypeFields := c.pgtypeFields()
	transcoders, err := makeValueTranscoders(connInfo, pgtypeFields)
	if err != nil {
		return nil, err
	}
	for i, field := range c.fields {
		elementTypeName := TypeFullName{Namespace: field.typeNamespace, Name: field.typeName}
		if mappedOID, ok := nameToOID[elementTypeName]; ok {
			logger.Log.Debug(
				"mapped composite type element to OID",
				log.String("composite_type_name", fmt.Sprintf("%s.%s", c.namespace, c.name)),
				log.Reflect("element_type_name", fmt.Sprintf("%s.%s", elementTypeName.Namespace, elementTypeName.Name)),
				log.Int32("element_type_mapped_oid", int32(mappedOID)),
			)
			pgtypeFields[i].OID = uint32(mappedOID)
		} else {
			logger.Log.Debug(
				"cannot map composite type element to OID",
				log.String("composite_type_name", fmt.Sprintf("%s.%s", c.namespace, c.name)),
				log.Reflect("element_type_name", fmt.Sprintf("%s.%s", elementTypeName.Namespace, elementTypeName.Name)),
			)
		}
	}
	value, _ := pgtype.NewCompositeTypeValues(c.name, pgtypeFields, transcoders)
	return value, nil
}

func (c *composite) pgtypeFields() (pgtypeFields []pgtype.CompositeTypeField) {
	for _, field := range c.fields {
		pgtypeFields = append(pgtypeFields, pgtype.CompositeTypeField{Name: field.fieldName, OID: uint32(field.oid)})
	}
	return pgtypeFields
}

func selectCompositeTypes(ctx context.Context, conn *pgx.Conn, complexTypes map[pgtype.OID]complexType) error {
	rows, err := conn.Query(ctx, compositeTypesQuery)
	if err != nil {
		return xerrors.Errorf("select: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var oid pgtype.OID
		var namespace, name pgtype.Text
		var field compositeTypeField
		if err := rows.Scan(&oid, &namespace, &name, &field.fieldName, &field.oid, &field.typeNamespace, &field.typeName); err != nil {
			return xerrors.Errorf("scan: %w", err)
		}

		typ, ok := complexTypes[pgtype.OID(oid)].(*composite)
		if !ok {
			complexTypes[pgtype.OID(oid)] = &composite{
				oid:       oid,
				namespace: namespace.String,
				name:      name.String,
				fields:    []compositeTypeField{field},
			}
		} else {
			typ.fields = append(typ.fields, field)
		}
	}
	return rows.Err()
}
