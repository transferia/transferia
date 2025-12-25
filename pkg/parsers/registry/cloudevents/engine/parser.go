package engine

import (
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/parsers"
	genericparser "github.com/transferia/transferia/pkg/parsers/generic"
	confluentschemaregistryengine "github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/engine"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type CloudEventsImpl struct {
	caCert                   string
	username                 string
	password                 string
	passwordFallback         string
	SendSrNotFoundToUnparsed bool
	logger                   log.Logger
	urlConverter             func(in string) string

	hostPortToClientMutex sync.Mutex
	hostPortToClient      map[string]*confluentschemaregistryengine.ConfluentSrImpl
}

var tableSchema *abstract.TableSchema

func init() {
	columns := []abstract.ColSchema{
		newColSchema("id", ytschema.TypeString, true),
		newColSchema("source", ytschema.TypeString, false),
		newColSchema("type", ytschema.TypeString, false),
		newColSchema("dataschema", ytschema.TypeString, false),
		newColSchema("subject", ytschema.TypeString, false),
		newColSchema("time", ytschema.TypeTimestamp, false), // time.Time
		newColSchema("payload", ytschema.TypeAny, false),
	}
	tableSchema = abstract.NewTableSchema(columns)
}

func newColSchema(columnName string, dataType ytschema.Type, isPkey bool) abstract.ColSchema {
	result := abstract.NewColSchema(columnName, dataType, false)
	result.Required = true
	if isPkey {
		result.PrimaryKey = true
	}
	return result
}

func buildChangeItem(
	changeItem *abstract.ChangeItem,
	fields *cloudEventsProtoFields,
	msg parsers.Message,
	partition abstract.Partition,
) abstract.ChangeItem {
	subject := fields.subject
	if len(subject) == 0 {
		subject = partition.Topic
	}
	return abstract.ChangeItem{
		ID:          0,
		LSN:         msg.Offset,
		CommitTime:  uint64(msg.WriteTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       strings.ReplaceAll(partition.Topic, "/", "_"),
		PartID:      "",
		ColumnNames: tableSchema.ColumnNames(),
		ColumnValues: []interface{}{
			fields.id,
			fields.source,
			fields.type_,
			fields.dataschema,
			subject,
			fields.time,
			changeItem.AsMap(),
		},
		TableSchema:      tableSchema,
		OldKeys:          abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
		Size:             abstract.RawEventSize(uint64(len(msg.Value))),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
}

func (p *CloudEventsImpl) getConfluentSRParserImpl(hostPort string) (*confluentschemaregistryengine.ConfluentSrImpl, error) {
	p.hostPortToClientMutex.Lock()
	defer p.hostPortToClientMutex.Unlock()

	if parser, ok := p.hostPortToClient[hostPort]; ok {
		return parser, nil
	}

	p.logger.Infof("try to create confluentSRParser for host/port:%s, username:%s", hostPort, p.username)

	confluentSRParser := confluentschemaregistryengine.NewConfluentSchemaRegistryImpl(hostPort, p.caCert, p.username, p.password, false, p.SendSrNotFoundToUnparsed, p.logger)
	isAuthorizedPrimaryPass, err := confluentSRParser.SchemaRegistryClient.IsAuthorized()
	if err != nil {
		return nil, xerrors.Errorf("unable to check if authorized with primary password, err: %w", err)
	}
	if isAuthorizedPrimaryPass {
		p.logger.Info("SchemaRegistry got original password")
		p.hostPortToClient[hostPort] = confluentSRParser
		return confluentSRParser, nil
	}
	p.logger.Info("tested original password, didn't work. Try fallback password")

	confluentSRParser = confluentschemaregistryengine.NewConfluentSchemaRegistryImpl(hostPort, p.caCert, p.username, p.passwordFallback, false, p.SendSrNotFoundToUnparsed, p.logger)
	isAuthorizedFallbackPass, err := confluentSRParser.SchemaRegistryClient.IsAuthorized()
	if err != nil {
		return nil, xerrors.Errorf("unable to check if authorized with fallback password, err: %w", err)
	}
	if isAuthorizedFallbackPass {
		p.logger.Info("SchemaRegistry got fallback password")
		p.hostPortToClient[hostPort] = confluentSRParser
		return confluentSRParser, nil
	}
	p.logger.Info("tested fallback password, didn't work. Try fallback password")

	return nil, xerrors.New("unable to authorize on primary & fallback password")
}

func (p *CloudEventsImpl) getConfluentSRParser(hostPort string) *confluentschemaregistryengine.ConfluentSrImpl {
	var confluentSRParser *confluentschemaregistryengine.ConfluentSrImpl
	_ = backoff.RetryNotify(func() error {
		var err error
		confluentSRParser, err = p.getConfluentSRParserImpl(hostPort)
		return err
	}, backoff.NewConstantBackOff(time.Second), util.BackoffLogger(p.logger, "making schema registry client"))
	return confluentSRParser
}

func (p *CloudEventsImpl) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	cloudEventsFields, body, protoPath, err := unpackCloudEventsProtoMessage(msg.Value)
	if err != nil {
		err := xerrors.Errorf("unable to unpack cloudEvents proto message, err: %w", err)
		changeItems := []abstract.ChangeItem{genericparser.NewUnparsed(partition, partition.Topic, string(msg.Value), err.Error(), 0, msg.Offset, msg.WriteTime)}
		return changeItems
	}

	hostPort, schemaID, err := extractSchemaIDAndURL(cloudEventsFields.dataschema)
	if err != nil {
		err := xerrors.Errorf("unable to break URL into subject&version, err: %w", err)
		changeItems := []abstract.ChangeItem{genericparser.NewUnparsed(partition, partition.Topic, string(msg.Value), err.Error(), 0, msg.Offset, msg.WriteTime)}
		return changeItems
	}

	if p.urlConverter != nil {
		hostPort = p.urlConverter(hostPort)
	}

	confluentSRParser := p.getConfluentSRParser(hostPort)
	_, leastChangeItems := confluentSRParser.DoWithSchemaID(partition, schemaID, protoPath, body, msg.Offset, msg.WriteTime, true)

	result := make([]abstract.ChangeItem, 0, len(leastChangeItems))
	for i, currChangeItem := range leastChangeItems {
		var finalChangeItem abstract.ChangeItem
		if strings.HasSuffix(currChangeItem.Table, "_unparsed") {
			finalChangeItem = currChangeItem
		} else {
			finalChangeItem = buildChangeItem(
				&currChangeItem,
				cloudEventsFields,
				msg,
				partition,
			)
		}
		finalChangeItem.FillQueueMessageMeta(partition.Topic, int(partition.Partition), msg.Offset, i)
		result = append(result, finalChangeItem)
	}
	return result
}

func (p *CloudEventsImpl) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0, len(batch.Messages))
	for _, msg := range batch.Messages {
		result = append(result, p.Do(msg, abstract.Partition{Cluster: "", Partition: batch.Partition, Topic: batch.Topic})...)
	}
	return result
}

func NewCloudEventsImpl(caCert string, username string, password string, passwordFallback string, SendSrNotFoundToUnparsed bool, logger log.Logger, urlConverter func(in string) string) *CloudEventsImpl {
	return &CloudEventsImpl{
		caCert:                   caCert,
		username:                 username,
		password:                 password,
		passwordFallback:         passwordFallback,
		SendSrNotFoundToUnparsed: SendSrNotFoundToUnparsed,
		logger:                   logger,
		urlConverter:             urlConverter,
		hostPortToClientMutex:    sync.Mutex{},
		hostPortToClient:         make(map[string]*confluentschemaregistryengine.ConfluentSrImpl),
	}
}
