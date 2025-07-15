package abstract

import (
	"github.com/transferia/transferia/pkg/abstract/changeitem"
)

type ChangeItem = changeitem.ChangeItem
type ColSchema = changeitem.ColSchema
type DBSchema = changeitem.DBSchema
type EventSize = changeitem.EventSize
type FastTableSchema = changeitem.FastTableSchema
type Kind = changeitem.Kind
type OldKeysType = changeitem.OldKeysType
type Partition = changeitem.Partition
type PropertyKey = changeitem.PropertyKey
type TableID = changeitem.TableID
type TablePartID = changeitem.TablePartID
type TableSchema = changeitem.TableSchema
type TxBound = changeitem.TxBound
type ColumnName = changeitem.ColumnName
type TableColumns = changeitem.TableColumns

//---

const TableLSN = changeitem.TableLSN
const TableConsumerKeeper = changeitem.TableConsumerKeeper
const OriginalTypeMirrorBinary = changeitem.OriginalTypeMirrorBinary

//---

var DropTableKind = changeitem.DropTableKind
var TruncateTableKind = changeitem.TruncateTableKind
var InitShardedTableLoad = changeitem.InitShardedTableLoad
var InitTableLoad = changeitem.InitTableLoad
var DoneTableLoad = changeitem.DoneTableLoad
var DoneShardedTableLoad = changeitem.DoneShardedTableLoad

var InsertKind = changeitem.InsertKind
var UpdateKind = changeitem.UpdateKind
var DeleteKind = changeitem.DeleteKind
var PgDDLKind = changeitem.PgDDLKind
var DDLKind = changeitem.DDLKind
var MongoUpdateDocumentKind = changeitem.MongoUpdateDocumentKind
var MongoCreateKind = changeitem.MongoCreateKind
var MongoDropKind = changeitem.MongoDropKind
var MongoRenameKind = changeitem.MongoRenameKind
var MongoDropDatabaseKind = changeitem.MongoDropDatabaseKind
var MongoNoop = changeitem.MongoNoop
var ChCreateTableKind = changeitem.ChCreateTableKind
var ClickhouseDDLBuilderKind = changeitem.ClickhouseDDLBuilderKind
var ElasticsearchDumpIndexKind = changeitem.ElasticsearchDumpIndexKind
var SynchronizeKind = changeitem.SynchronizeKind

var RawDataColsIDX = changeitem.RawDataColsIDX
var RawMessageTopic = changeitem.RawMessageTopic
var RawMessagePartition = changeitem.RawMessagePartition
var RawMessageSeqNo = changeitem.RawMessageSeqNo
var RawMessageWriteTime = changeitem.RawMessageWriteTime

//---

var NewTableSchema = changeitem.NewTableSchema
var Dump = changeitem.Dump
var Collapse = changeitem.Collapse
var SplitByID = changeitem.SplitByID
var SplitUpdatedPKeys = changeitem.SplitUpdatedPKeys
var NewPartition = changeitem.NewPartition
var NewTableID = changeitem.NewTableID
var EmptyOldKeys = changeitem.EmptyOldKeys
var EmptyEventSize = changeitem.EmptyEventSize
var IsSystemTable = changeitem.IsSystemTable
var NewColSchema = changeitem.NewColSchema
var ContainsNonRowItem = changeitem.ContainsNonRowItem
var GetRawMessageData = changeitem.GetRawMessageData
var SplitByTableID = changeitem.SplitByTableID
var RawEventSize = changeitem.RawEventSize
var Sniff = changeitem.Sniff
var MakeFastTableSchema = changeitem.MakeFastTableSchema
var MakeMapColNameToIndex = changeitem.MakeMapColNameToIndex
var MakeTypedColSchema = changeitem.MakeTypedColSchema
var FindItemOfKind = changeitem.FindItemOfKind
var KeyNames = changeitem.KeyNames
var RegisterSystemTables = changeitem.RegisterSystemTables
var MakeRawMessage = changeitem.MakeRawMessage
var PgName = changeitem.PgName
var MakeOriginallyTypedColSchema = changeitem.MakeOriginallyTypedColSchema
var ColIDX = changeitem.ColIDX
var RawDataSchema = changeitem.RawDataSchema
var ChangeItemFromMap = changeitem.ChangeItemFromMap
