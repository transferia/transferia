package engine

import (
	"strings"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/schemaregistry/confluent"
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

func parseRecordName(in string) (string, string, error) {
	separatedTitle := strings.SplitN(in, ".", 4)
	if len(separatedTitle) != 4 {
		return "", "", xerrors.Errorf("Can't split recordName %q into schema and table names", in)
	}
	schemaName := separatedTitle[1]
	tableName := separatedTitle[2]
	return schemaName, tableName, nil
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

func makeChangeItemsFromMessageWithProtobuf(inMDBuilder *mdBuilder, schema *confluent.Schema, refs map[string]confluent.Schema, messageName string, buf []byte, offset uint64, writeTime time.Time, isCloudevents bool) ([]abstract.ChangeItem, error) {
	currBuf := buf
	if !isCloudevents {
		arrayIndexesFirstByte := buf[0]
		if arrayIndexesFirstByte != 0 {
			return nil, xerrors.Errorf("for now supported only case, when 'message indexes' is zero byte")
		}
		currBuf = buf[1:]
	}

	messageDescriptor, recordName, err := inMDBuilder.toMD(schema, refs, messageName)
	if err != nil {
		return nil, xerrors.Errorf("unable to build MessageDescriptor, err: %w", err)
	}
	dynamicMessage := dynamic.NewMessage(messageDescriptor)

	if !isCloudevents {
		currBuf = currBuf[0 : len(currBuf)-1] // it ends with '\n'
	}

	err = dynamicMessage.Unmarshal(currBuf)
	if err != nil {
		return nil, xerrors.Errorf("unable to unmarshal message, err: %w", err)
	}

	var schemaName, tableName string
	if !isCloudevents {
		schemaName, tableName, err = parseRecordName(recordName)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse record name, recordName: %s, err: %w", recordName, err)
		}
	}

	tableColumns, names, values, err := unpackProtobufDynamicMessage(schemaName, tableName, dynamicMessage)
	if err != nil {
		return nil, xerrors.Errorf("Can't process payload:%w", err)
	}
	changeItem := abstract.ChangeItem{
		ID:           0,
		LSN:          offset,
		CommitTime:   uint64(writeTime.UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       schemaName,
		Table:        tableName,
		PartID:       "",
		ColumnNames:  names,
		ColumnValues: values,
		TableSchema:  tableColumns,
		OldKeys:      abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
		TxID:         "",
		Query:        "",
		Size:         abstract.RawEventSize(uint64(len(buf))),
	}
	return []abstract.ChangeItem{changeItem}, nil
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
