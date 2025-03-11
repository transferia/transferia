package engine

import (
	"unicode/utf8"

	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/parsers"
	"go.ytsaurus.tech/library/go/core/log"
)

type kafkaConfig struct {
	isAddTimestamp bool
	isAddHeaders   bool
	isAddKey       bool
	isKeyString    bool
	isValueString  bool
	tableName      string
	isTopicAsName  bool
}

type RawToTableImpl struct {
	logger      log.Logger
	cfg         *kafkaConfig
	dlq         *RawToTableImpl
	tableSchema *abstract.TableSchema
	columnNames []string
}

func (p *RawToTableImpl) isSendToDLQ(msg parsers.Message) bool {
	if p.cfg.isAddKey && p.cfg.isKeyString && !utf8.Valid(msg.Key) {
		return true
	}
	if p.cfg.isValueString && !utf8.Valid(msg.Value) {
		return true
	}
	return false
}

func (p *RawToTableImpl) buildColumnValues(msg parsers.Message, partition abstract.Partition) []interface{} {
	columnValues := make([]interface{}, 0, len(p.columnNames))
	columnValues = append(columnValues, partition.Topic)
	columnValues = append(columnValues, partition.Partition)
	columnValues = append(columnValues, uint32(msg.Offset))
	if p.cfg.isAddTimestamp {
		columnValues = append(columnValues, msg.WriteTime)
	}
	if p.cfg.isAddHeaders {
		columnValues = append(columnValues, msg.Headers)
	}
	if p.cfg.isAddKey {
		if p.cfg.isKeyString {
			columnValues = append(columnValues, string(msg.Key))
		} else {
			columnValues = append(columnValues, msg.Key)
		}
	}
	if p.cfg.isValueString {
		columnValues = append(columnValues, string(msg.Value))
	} else {
		columnValues = append(columnValues, msg.Value)
	}
	return columnValues
}

func (p *RawToTableImpl) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	if p.dlq != nil && p.isSendToDLQ(msg) {
		return p.dlq.Do(msg, partition)
	}

	columnValues := p.buildColumnValues(msg, partition)

	table := p.cfg.tableName
	if p.cfg.isTopicAsName {
		table = partition.Topic
	}
	return []abstract.ChangeItem{{
		ID:           0,
		LSN:          msg.Offset,
		CommitTime:   uint64(msg.WriteTime.UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       "",
		Table:        table,
		PartID:       "",
		ColumnNames:  p.columnNames,
		ColumnValues: columnValues,
		TableSchema:  p.tableSchema,
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{ColNameTopic, ColNamePartition, ColNameOffset},
			KeyTypes:  []string{"", "", ""},
			KeyValues: []interface{}{partition.Topic, partition.Partition, msg.Offset},
		},
		TxID:  "",
		Query: "",
		Size:  abstract.RawEventSize(uint64(len(msg.Value))),
	}}
}

func (p *RawToTableImpl) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0, 1000)
	for _, msg := range batch.Messages {
		result = append(result, p.Do(msg, abstract.Partition{Cluster: "", Partition: batch.Partition, Topic: batch.Topic})...)
	}
	return result
}

func newRawToTableImpl(logger log.Logger, isAddTimestamp, isAddHeaders, isAddKey, isKeyString, isValueString, isTopicAsName bool, tableName string, isDLQ bool) *RawToTableImpl {
	cfg := &kafkaConfig{
		isAddTimestamp: isAddTimestamp,
		isAddHeaders:   isAddHeaders,
		isAddKey:       isAddKey,
		isKeyString:    isKeyString,
		isValueString:  isValueString,
		isTopicAsName:  isTopicAsName,
		tableName:      tableName,
	}
	tableSchema, columnNames := buildTableSchemaAndColumnNames(cfg)

	var dlq *RawToTableImpl = nil
	if !isDLQ {
		dlq = NewRawToTableDLQ(logger, tableName)
	}

	return &RawToTableImpl{
		logger:      logger,
		cfg:         cfg,
		dlq:         dlq,
		tableSchema: tableSchema,
		columnNames: columnNames,
	}
}

func NewRawToTableDLQ(logger log.Logger, tableName string) *RawToTableImpl {
	cfg := &kafkaConfig{
		isAddTimestamp: true,
		isAddHeaders:   true,
		isAddKey:       true,
		isKeyString:    false,
		isValueString:  false,
		tableName:      tableName + "_dlq",
		isTopicAsName:  false,
	}
	return newRawToTableImpl(logger, cfg.isAddTimestamp, cfg.isAddKey, cfg.isKeyString, cfg.isValueString, cfg.isAddHeaders, cfg.isTopicAsName, cfg.tableName, true)
}

func NewRawToTable(logger log.Logger, isAddTimestamp, isAddHeaders, isAddKey, isKeyString, isValueString, isTopicAsName bool, tableName string) *RawToTableImpl {
	return newRawToTableImpl(logger, isAddTimestamp, isAddHeaders, isAddKey, isKeyString, isValueString, isTopicAsName, tableName, false)
}
