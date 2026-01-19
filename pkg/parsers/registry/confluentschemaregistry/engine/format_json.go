package engine

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/table_name_policy"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
)

func makeChangeItemsFromMessageWithJSON(
	schema *confluent.Schema,
	buf []byte,
	offset uint64,
	writeTime time.Time,
	isGenerateUpdates bool,
	tableNamePolicy table_name_policy.TableNamePolicy,
) ([]abstract.ChangeItem, int, error) {
	var jsonProperties JSONProperties
	err := json.Unmarshal([]byte(schema.Schema), &jsonProperties)
	if err != nil {
		return nil, 0, xerrors.Errorf("Can't unmarshal JSON schema %s: %w", schema.Schema, err)
	}

	schemaName, tableName, err := table_name_policy.BuildJSONTableID(tableNamePolicy, jsonProperties.Title)
	if err != nil {
		return nil, 0, xerrors.Errorf("Can't build JSON table ID, err: %w", err)
	}

	msgLen := len(buf)
	zeroIndex := bytes.Index(buf, []byte{0})
	if zeroIndex != -1 {
		msgLen = zeroIndex
	}

	tableColumns, names, values, err := processPayload(schemaName, tableName, &jsonProperties, buf[0:msgLen], isGenerateUpdates)
	if err != nil {
		return nil, 0, xerrors.Errorf("Can't process payload:%w", err)
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
	return []abstract.ChangeItem{changeItem}, msgLen, nil
}
