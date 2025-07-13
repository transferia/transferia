package blank

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	PartitionColum   = "partition"
	OffsetColumn     = "offset"
	SeqNoColumn      = "seq_no"
	CreateTimeColumn = "c_time"
	WriteTimeColumn  = "w_time"
	IPColumn         = "ip"
	RawMessageColumn = "lb_raw_message"
	SourceIDColumn   = "source_id"
	ExtrasColumn     = "lb_extra_fields"
)

var (
	BlankCols    = cols(BlankSchema.Columns())
	BlankColsIDX = colIDX(BlankSchema.Columns())
)

func ExtractValue[T any](ci *abstract.ChangeItem, col string) (T, error) {
	var tt T
	v := ci.ColumnValues[BlankColsIDX[col]]
	vv, ok := v.(T)
	if !ok {
		return tt, xerrors.Errorf("expect %T for %s, recieve: %T", tt, col, v)
	}
	return vv, nil
}

func cols(blankSchema []abstract.ColSchema) []string {
	res := make([]string, len(blankSchema))
	for i := range blankSchema {
		res[i] = blankSchema[i].ColumnName
	}
	return res
}

func colIDX(schema []abstract.ColSchema) map[string]int {
	res := map[string]int{}
	for i := range schema {
		res[schema[i].ColumnName] = i
	}
	return res
}

var (
	BlankSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: PartitionColum, PrimaryKey: true, DataType: "string"},
		{ColumnName: OffsetColumn, PrimaryKey: true, DataType: "uint64"},
		{ColumnName: SeqNoColumn, DataType: "uint64"},
		{ColumnName: SourceIDColumn, DataType: "string"},
		{ColumnName: CreateTimeColumn, DataType: "datetime"},
		{ColumnName: WriteTimeColumn, DataType: "datetime"},
		{ColumnName: IPColumn, DataType: "string"},
		{ColumnName: RawMessageColumn, DataType: "string"},
		{ColumnName: ExtrasColumn, DataType: "any"},
	})
)

func NewRawMessage(msg parsers.Message, partition abstract.Partition) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         msg.SeqNo,
		CommitTime:  uint64(msg.WriteTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       partition.String(),
		PartID:      "",
		ColumnNames: BlankCols,
		ColumnValues: []interface{}{
			partition.String(),
			msg.Offset,
			msg.SeqNo,
			string(msg.Key),
			msg.CreateTime,
			msg.WriteTime,
			"", // IP not replicated anymore
			msg.Value,
			msg.Headers,
		},
		TableSchema:      BlankSchema,
		OldKeys:          abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
		Size:             abstract.RawEventSize(uint64(len(msg.Value))),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
}

//---

type ParserBlank struct {
}

func (p *ParserBlank) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	var res []abstract.ChangeItem
	partition := abstract.NewPartition(batch.Topic, batch.Partition)
	for _, msg := range batch.Messages {
		res = append(res, p.Do(msg, partition)...)
	}
	return res
}

func (p *ParserBlank) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	return []abstract.ChangeItem{NewRawMessage(msg, partition)}
}

func NewParserBlank(_ interface{}, _ bool, logger log.Logger, registry *stats.SourceStats) (parsers.Parser, error) {
	return &ParserBlank{}, nil
}

func init() {
	parsers.Register(
		NewParserBlank,
		[]parsers.AbstractParserConfig{new(ParserConfigBlankLb)},
	)
}
