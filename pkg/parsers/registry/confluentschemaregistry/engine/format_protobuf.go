package engine

import (
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/table_name_policy"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
)

func makeChangeItemsFromMessageWithProtobuf(
	inMDBuilder *mdBuilder,
	schema *confluent.Schema,
	refs map[string]confluent.Schema,
	inMessageName string, // not-empty string is only for case 'cloudevents'
	buf []byte,
	offset uint64,
	writeTime time.Time,
	isCloudevents bool,
	isGenerateUpdates bool,
	tableNamePolicy table_name_policy.TableNamePolicy,
) ([]abstract.ChangeItem, error) {
	currBuf := buf
	messageName := inMessageName
	if !isCloudevents {
		arrayIndexesFirstByte := buf[0]
		if arrayIndexesFirstByte == 0 {
			// leave 'messageName' as is
			currBuf = buf[1:]
		} else {
			newMessageName, leastBuf, err := handleMessageIndexes(buf, schema)
			if err != nil {
				return nil, xerrors.Errorf("unable to handle message indexes, err: %w", err)
			}
			// found-out 'messageName'
			messageName = newMessageName
			currBuf = leastBuf
		}
	}

	messageDescriptor, messageFullName, err := inMDBuilder.toMD(schema, refs, messageName)
	if err != nil {
		return nil, xerrors.Errorf("unable to build MessageDescriptor, err: %w", err)
	}
	dynamicMessage := dynamic.NewMessage(messageDescriptor)

	err = dynamicMessage.Unmarshal(currBuf)
	if err != nil {
		return nil, xerrors.Errorf("unable to unmarshal message, err: %w", err)
	}

	var schemaName, tableName string
	if !isCloudevents {
		schemaName, tableName, err = table_name_policy.BuildProtobufTableID(tableNamePolicy, messageFullName)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse record name, messageFullName: %s, err: %w", messageFullName, err)
		}
	}

	tableColumns, names, values, err := unpackProtobufDynamicMessage(schemaName, tableName, dynamicMessage)
	if err != nil {
		return nil, xerrors.Errorf("Can't process payload:%w", err)
	}
	kind := abstract.InsertKind
	if isGenerateUpdates {
		kind = abstract.UpdateKind
	}
	changeItem := abstract.ChangeItem{
		ID:               0,
		LSN:              offset,
		CommitTime:       uint64(writeTime.UnixNano()),
		Counter:          0,
		Kind:             kind,
		Schema:           schemaName,
		Table:            tableName,
		PartID:           "",
		ColumnNames:      names,
		ColumnValues:     values,
		TableSchema:      tableColumns,
		OldKeys:          abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
		Size:             abstract.RawEventSize(uint64(len(buf))),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
	return []abstract.ChangeItem{changeItem}, nil
}
