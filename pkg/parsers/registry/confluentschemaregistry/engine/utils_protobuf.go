package engine

import (
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/engine/protobuf_extractor"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"github.com/transferia/transferia/pkg/util"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var BuiltInDeps map[string]string //nolint:gochecknoglobals // read-only entity

func protoMap(fileName, fileContent string) map[string]string {
	result := make(map[string]string)
	for k, v := range BuiltInDeps {
		result[k] = v
	}
	result[fileName] = fileContent
	return result
}

func getRecordName(in *desc.FileDescriptor) string {
	for _, el := range in.GetMessageTypes() {
		return el.GetFullyQualifiedName()
	}
	return ""
}

func dirtyPatch(in string) string {
	if !strings.Contains(in, `confluent.`) {
		return in
	}
	lines := strings.Split(in, "\n")
	index := -1
	for i := 0; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "package ") {
			index = i
			break
		}
	}
	if index == -1 {
		return in
	}
	result := make([]string, 0, len(lines)+1)
	result = append(result, lines[0:index+1]...)
	result = append(result, `import "confluent/meta.proto";`)
	result = append(result, lines[index+1:]...)
	return strings.Join(result, "\n")
}

func handleField(schemaName, tableName, colName, protoType string, isRequired, isRepeated bool) (abstract.ColSchema, error) {
	var colType ytschema.Type
	if isRepeated {
		colType = ytschema.TypeAny
	} else {
		var ok bool
		colType, ok = protoSchemaTypes[protoType]
		if !ok {
			return abstract.ColSchema{}, xerrors.Errorf("unknown proto type: %s", protoType)
		}
	}
	return abstract.ColSchema{
		TableSchema:  schemaName,
		TableName:    tableName,
		Path:         "",
		ColumnName:   colName,
		DataType:     colType.String(),
		PrimaryKey:   false,
		FakeKey:      false,
		Required:     isRequired,
		Expression:   "",
		OriginalType: "",
		Properties:   nil,
	}, nil
}

func unpackProtobufDynamicMessage(schemaName, tableName string, dynamicMessage *dynamic.Message) (*abstract.TableSchema, []string, []interface{}, error) {
	var colSchema []abstract.ColSchema
	var names []string
	var values []interface{}
	for _, currField := range dynamicMessage.GetKnownFields() {
		currFieldName := currField.GetName()
		currFieldType := currField.GetType().String()
		isRepeated := currField.IsRepeated()

		currColSchema, err := handleField(schemaName, tableName, currFieldName, currFieldType, currField.IsRequired(), isRepeated)
		if err != nil {
			return nil, nil, nil, xerrors.Errorf("unable to handle field %s, err: %w", currFieldName, err)
		}

		val, err := unpackVal(dynamicMessage.GetFieldByName(currFieldName), currFieldType, isRepeated)
		if err != nil {
			return nil, nil, nil, xerrors.Errorf("unable to unpack field %s, err: %w", currFieldName, err)
		}
		colSchema = append(colSchema, currColSchema)
		names = append(names, currFieldName)
		values = append(values, val)
	}
	return abstract.NewTableSchema(colSchema), names, values, nil
}

func handleMessageIndexes(inBuf []byte, schema *confluent.Schema) (string, []byte, error) {
	messageIndexes, leastBuf, err := extractMessageIndexes(inBuf)
	if err != nil {
		return "", nil, xerrors.Errorf("unable to extract message indexes, err: %w", err)
	}
	fullName, err := protobuf_extractor.ExtractMessageFullNameByIndex(schema.Schema, messageIndexes)
	if err != nil {
		return "", nil, xerrors.Errorf("unable to extract message full name, err: %w", err)
	}
	return fullName, leastBuf, nil
}

func extractMessageIndexes(inBuf []byte) ([]int, []byte, error) {
	if len(inBuf) == 0 {
		return nil, nil, xerrors.Errorf("empty input")
	}

	arrayLen, consumed, err := util.ZigzagVarIntDecode(inBuf)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to extract array length, err: %w", err)
	}
	currBuf := inBuf[consumed:]
	resultArr := make([]int, 0, arrayLen)
	for i := 0; i < int(arrayLen); i++ {
		var nextVal int64
		nextVal, consumed, err = util.ZigzagVarIntDecode(currBuf)
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to handle message indexes, err: %w", err)
		}
		currBuf = currBuf[consumed:]
		resultArr = append(resultArr, int(nextVal))
	}
	return resultArr, currBuf, nil
}
