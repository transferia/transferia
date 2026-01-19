package engine

import (
	"encoding/binary"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	genericparser "github.com/transferia/transferia/pkg/parsers/generic"
	"github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/table_name_policy"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"github.com/transferia/transferia/pkg/schemaregistry/warmup"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type ConfluentSrImpl struct {
	logger                    log.Logger
	SchemaRegistryClient      *confluent.SchemaRegistryClient
	schemaRegistryClientMutex sync.Mutex
	isGenerateUpdates         bool
	tableNamePolicy           table_name_policy.TableNamePolicy
	sendSrNotFoundToUnparsed  bool
	inMDBuilder               *mdBuilder
}

func (p *ConfluentSrImpl) doWithSchema(
	partition abstract.Partition,
	schema *confluent.Schema,
	refs map[string]confluent.Schema,
	messageName string, // not-empty string is only for case 'cloudevents'
	buf []byte,
	offset uint64,
	writeTime time.Time,
	isCloudevents bool,
) ([]byte, []abstract.ChangeItem) {
	var changeItems []abstract.ChangeItem
	var msgLen int
	var err error
	switch schema.SchemaType {
	case confluent.JSON:
		changeItems, msgLen, err = makeChangeItemsFromMessageWithJSON(schema, buf, offset, writeTime, p.isGenerateUpdates, p.tableNamePolicy)
	case confluent.PROTOBUF:
		changeItems, err = makeChangeItemsFromMessageWithProtobuf(p.inMDBuilder, schema, refs, messageName, buf, offset, writeTime, isCloudevents, p.isGenerateUpdates, p.tableNamePolicy)
		msgLen = len(buf)
	default:
		err = xerrors.Errorf("Schema type is not JSON/PROTOBUF (%v) (currently only the json & protobuf schema is supported)", schema.SchemaType)
	}
	if err != nil {
		errStr := xerrors.Errorf("Can't make change item from message %w", err).Error()
		changeItems = []abstract.ChangeItem{genericparser.NewUnparsed(partition, partition.Topic, buf, errStr, 0, offset, writeTime)}
		return nil, changeItems
	}
	return buf[msgLen:], changeItems
}

func (p *ConfluentSrImpl) DoWithSchemaID(
	partition abstract.Partition,
	schemaID uint32,
	messageName string, // not-empty string is only for case 'cloudevents'
	buf []byte,
	offset uint64,
	writeTime time.Time,
	isCloudevents bool,
) ([]byte, []abstract.ChangeItem) {
	is404 := false

	var currSchema *confluent.Schema
	_ = backoff.RetryNotify(func() error {
		var err error
		currSchema, err = p.SchemaRegistryClient.GetSchema(int(schemaID)) // returns *Schema

		if p.sendSrNotFoundToUnparsed && err != nil && strings.Contains(err.Error(), "Error code: 404") {
			is404 = true
			return nil
		}
		return err
	}, backoff.NewConstantBackOff(time.Second), util.BackoffLogger(p.logger, "getting schema"))

	if p.sendSrNotFoundToUnparsed && is404 {
		errStr := xerrors.Errorf("SchemaRegistry for schema (id: %v) returned http code 404", schemaID).Error()
		return nil, []abstract.ChangeItem{genericparser.NewUnparsed(partition, partition.Topic, buf, errStr, 0, offset, writeTime)}
	}

	if currSchema.SchemaType.String() == "" {
		errStr := xerrors.Errorf("Schema type for schema (id: %v) not defined", schemaID).Error()
		return nil, []abstract.ChangeItem{genericparser.NewUnparsed(partition, partition.Topic, buf, errStr, 0, offset, writeTime)}
	}

	// handle 'references', if present
	var refs map[string]confluent.Schema = nil
	var err error
	if currSchema != nil && len(currSchema.References) != 0 {
		refs, err = p.SchemaRegistryClient.ResolveReferencesRecursive(currSchema.References)
		if err != nil {
			errStr := xerrors.Errorf("ResolveReferencesRecursive for schema (id: %v) returned error, %w", schemaID, err).Error()
			return nil, []abstract.ChangeItem{genericparser.NewUnparsed(partition, partition.Topic, buf, errStr, 0, offset, writeTime)}
		}
	}

	return p.doWithSchema(partition, currSchema, refs, messageName, buf, offset, writeTime, isCloudevents)
}

func (p *ConfluentSrImpl) DoOne(partition abstract.Partition, buf []byte, offset uint64, writeTime time.Time) ([]byte, []abstract.ChangeItem) {
	if len(buf) < 5 {
		errStr := xerrors.Errorf("Can't extract schema id form message: message length less then 5 (%v)", len(buf)).Error()
		return nil, []abstract.ChangeItem{genericparser.NewUnparsed(partition, partition.Topic, buf, errStr, 0, offset, writeTime)}
	}
	if buf[0] != 0 {
		errStr := xerrors.Errorf("Unknown magic byte in message (%v) (first byte in message must be 0)", string(buf)).Error()
		return nil, []abstract.ChangeItem{genericparser.NewUnparsed(partition, partition.Topic, buf, errStr, 0, offset, writeTime)}
	}
	schemaID := binary.BigEndian.Uint32(buf[1:5])
	bufWithoutWirePrefix := buf[5:]
	return p.DoWithSchemaID(partition, schemaID, "", bufWithoutWirePrefix, offset, writeTime, false)
}

func (p *ConfluentSrImpl) DoBuf(partition abstract.Partition, buf []byte, offset uint64, writeTime time.Time) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0)
	leastBuf := buf
	for {
		if len(leastBuf) == 0 {
			break
		}
		var changeItems []abstract.ChangeItem
		leastBuf, changeItems = p.DoOne(partition, leastBuf, offset, writeTime)
		result = append(result, changeItems...)
	}
	return result
}

func (p *ConfluentSrImpl) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	result := p.DoBuf(partition, msg.Value, msg.Offset, msg.WriteTime)
	for i := range result {
		result[i].FillQueueMessageMeta(partition.Topic, int(partition.Partition), msg.Offset, i)
	}
	return result
}

func (p *ConfluentSrImpl) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	warmup.WarmUpSRCache(p.logger, &p.schemaRegistryClientMutex, batch, p.SchemaRegistryClient, p.sendSrNotFoundToUnparsed)
	result := make([]abstract.ChangeItem, 0, len(batch.Messages))
	for _, msg := range batch.Messages {
		result = append(result, p.Do(msg, abstract.Partition{Cluster: "", Partition: batch.Partition, Topic: batch.Topic})...)
	}
	return result
}

func NewConfluentSchemaRegistryImpl(
	srURL string,
	caCert string,
	username string,
	password string,
	isGenerateUpdates bool,
	tableNamePolicy table_name_policy.TableNamePolicy,
	sendSrNotFoundToUnparsed bool,
	logger log.Logger,
) *ConfluentSrImpl {
	client, err := confluent.NewSchemaRegistryClientWithTransport(srURL, caCert, logger)
	if err != nil {
		logger.Warnf("Unable to create schema registry client: %v", err)
		return nil
	}
	client.SetCredentials(username, password)
	return &ConfluentSrImpl{
		logger:                    logger,
		SchemaRegistryClient:      client,
		schemaRegistryClientMutex: sync.Mutex{},
		isGenerateUpdates:         isGenerateUpdates,
		tableNamePolicy:           tableNamePolicy,
		sendSrNotFoundToUnparsed:  sendSrNotFoundToUnparsed,
		inMDBuilder:               newMDBuilder(),
	}
}
