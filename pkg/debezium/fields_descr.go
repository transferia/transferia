package debezium

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	"github.com/transferia/transferia/pkg/debezium/mysql"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/debezium/pg"
	"github.com/transferia/transferia/pkg/debezium/ydb"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
)

type fieldsDescr struct {
	V []map[string]interface{}
}

func getFieldDescr(colSchema abstract.ColSchema, connectorParameters map[string]string, intoArray, snapshot bool) (map[string]interface{}, error) {
	var typeDescr *debeziumcommon.KafkaTypeDescr
	var err error
	var originalTypeProperties map[string]string
	if colSchema.OriginalType == "" {
		typeDescr, err = colSchemaToOriginalType(&colSchema)
		if err != nil {
			return nil, xerrors.Errorf("unable to get type description, err: %w", err)
		}
	} else if strings.HasPrefix(colSchema.OriginalType, "pg:") {
		typeDescr, err = pg.GetKafkaTypeDescrByPgType(&colSchema)
		if err != nil {
			return nil, xerrors.Errorf("unable to get pg fieldDescr: %s, err: %w", colSchema.OriginalType, err)
		}
		originalTypeProperties = pg.GetOriginalTypeProperties(&colSchema)
	} else if strings.HasPrefix(colSchema.OriginalType, "mysql:") {
		typeDescr, err = mysql.GetKafkaTypeDescrByMysqlType(colSchema.OriginalType)
		if err != nil {
			return nil, xerrors.Errorf("unable to get mysql fieldDescr: %s, err: %w", colSchema.OriginalType, err)
		}
	} else if strings.HasPrefix(colSchema.OriginalType, "ydb:") {
		typeDescr, err = ydb.GetKafkaTypeDescrByYDBType(colSchema.OriginalType)
		if err != nil {
			return nil, xerrors.Errorf("unable to get ydb fieldDescr: %s, err: %w", colSchema.OriginalType, err)
		}
	} else {
		return nil, xerrors.Errorf("unknown original type: %s", colSchema.OriginalType)
	}

	kafkaType, debeziumName, extra := typeDescr.KafkaTypeAndDebeziumNameAndExtra(&colSchema, intoArray, snapshot, connectorParameters)

	fieldDescr := make(map[string]interface{})
	fieldDescr["type"] = kafkaType
	fieldDescr["optional"] = !colSchema.IsKey()

	if !intoArray {
		fieldDescr["field"] = colSchema.ColumnName
	}

	if debeziumName != "" {
		fieldDescr["name"] = debeziumName
		fieldDescr["version"] = 1
	}

	for k, v := range extra {
		fieldDescr[k] = v
	}

	if debeziumparameters.GetDTAddOriginalTypeInfo(connectorParameters) == debeziumparameters.BoolTrue {
		originalTypeInfo := make(map[string]interface{})
		originalTypeInfo["original_type"] = colSchema.OriginalType

		if originalTypeProperties != nil {
			originalTypeInfo["properties"] = originalTypeProperties
		}

		fieldDescr["__dt_original_type_info"] = originalTypeInfo
	}

	return fieldDescr, nil
}

func (d *fieldsDescr) AddFieldDescr(colSchema abstract.ColSchema, snapshot bool, connectorParameters map[string]string) error {
	fieldDescr := make(map[string]interface{})
	var err error

	if strings.HasSuffix(colSchema.OriginalType, "[]") {
		elemDescr, err := getFieldDescr(pgcommon.BuildColSchemaArrayElement(colSchema), connectorParameters, true, snapshot)
		if err != nil {
			return xerrors.Errorf("unable to get array element field descr: %w", err)
		}
		fieldDescr["items"] = elemDescr
		fieldDescr["field"] = colSchema.ColumnName
		fieldDescr["type"] = "array"
		fieldDescr["optional"] = !colSchema.IsKey()

		if debeziumparameters.GetDTAddOriginalTypeInfo(connectorParameters) == debeziumparameters.BoolTrue {
			originalTypeInfo := make(map[string]string)
			originalTypeInfo["original_type"] = colSchema.OriginalType
			fieldDescr["__dt_original_type_info"] = originalTypeInfo
		}
	} else {
		fieldDescr, err = getFieldDescr(colSchema, connectorParameters, false, snapshot)
		if err != nil {
			return xerrors.Errorf("unable to get field descr: %w", err)
		}
	}

	d.V = append(d.V, fieldDescr)
	return nil
}

func newFields() *fieldsDescr {
	return &fieldsDescr{
		V: make([]map[string]interface{}, 0),
	}
}
