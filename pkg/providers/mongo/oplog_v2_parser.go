//go:build !disable_mongo_provider

package mongo

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.ytsaurus.tech/library/go/core/log"
)

const OplogProtocolVersion = 2

// protocol is actual for oplog v2, mongo v3.6.23
type oplogRsChangeEventV2 struct {
	Timestamp     primitive.Timestamp `bson:"ts"`
	Hash          int64               `bson:"h"`
	Version       int                 `bson:"v"`
	OperationType string              `bson:"op"`
	NamespaceRaw  string              `bson:"ns"`
	Object        bson.Raw            `bson:"o"`
	Object2       bson.Raw            `bson:"o2"`
}

type oplogRsChangeEventV2CommandType string

const (
	oplogRsChangeEventV2CommandTypeUnknown      oplogRsChangeEventV2CommandType = "unknown"
	oplogRsChangeEventV2CommandTypeCreate       oplogRsChangeEventV2CommandType = "create"
	oplogRsChangeEventV2CommandTypeRename       oplogRsChangeEventV2CommandType = "rename"
	oplogRsChangeEventV2CommandTypeDrop         oplogRsChangeEventV2CommandType = "drop"
	oplogRsChangeEventV2CommandTypeDropdatabase oplogRsChangeEventV2CommandType = "dropDatabase"
)

type oplogRsChangeEventV2CommandSelector struct {
	Create           string `bson:"create"`
	RenameCollection string `bson:"renameCollection"`
	Drop             string `bson:"drop"`
	DropDatabase     int    `bson:"dropDatabase"`

	RenamedTo string `bson:"to"` // for rename collection
}

func (o oplogRsChangeEventV2CommandSelector) toType() oplogRsChangeEventV2CommandType {
	switch {
	case o.Create != "" && o.RenameCollection == "" && o.Drop == "" && o.DropDatabase == 0:
		return oplogRsChangeEventV2CommandTypeCreate
	case o.Create == "" && o.RenameCollection != "" && o.Drop == "" && o.DropDatabase == 0:
		return oplogRsChangeEventV2CommandTypeRename
	case o.Create == "" && o.RenameCollection == "" && o.Drop != "" && o.DropDatabase == 0:
		return oplogRsChangeEventV2CommandTypeDrop
	case o.Create == "" && o.RenameCollection == "" && o.Drop == "" && o.DropDatabase != 0:
		return oplogRsChangeEventV2CommandTypeDropdatabase
	}
	return oplogRsChangeEventV2CommandTypeUnknown
}

func newNoopChangeEvent(clusterTime primitive.Timestamp) *KeyChangeEvent {
	var documentKey DocumentKey
	var namespace, toNamespace Namespace
	return &KeyChangeEvent{
		OperationType: "noop",
		DocumentKey:   documentKey,
		Namespace:     namespace,
		ToNamespace:   toNamespace,
		ClusterTime:   clusterTime,
	}
}

// Note: this function DOES NOT extract full document for one reason:
// update commands do not contain full documents, but mongo commands to modify existing document
func (e oplogRsChangeEventV2) toMongoKeyChangeEvent(log log.Logger) (*KeyChangeEvent, error) {
	if e.Version != OplogProtocolVersion {
		return nil, xerrors.Errorf("version %d of oplog protocol is not supported. Oplog keyEvent: %v", e.Version, e)
	}
	originalNs := ParseNamespace(e.NamespaceRaw)
	var operationType string
	var documentKey DocumentKey
	var namespace, toNamespace Namespace

	switch e.OperationType {
	case "i":
		var doc bson.D
		if err := bson.Unmarshal(e.Object, &doc); err != nil {
			return nil, xerrors.Errorf("document unmarshal error in operation type %s: %w", e.OperationType, err)
		}
		operationType, namespace, documentKey = "insert", *originalNs, DocumentKey{ID: doc.Map()["_id"]}
	case "u":
		var key DocumentKey
		if err := bson.Unmarshal(e.Object2, &key); err != nil {
			return nil, xerrors.Errorf("document key unmarshal error in operation type %s: %w", e.OperationType, err)
		}
		operationType, namespace, documentKey = "update", *originalNs, key
	case "d":
		var key DocumentKey
		if err := bson.Unmarshal(e.Object, &key); err != nil {
			return nil, xerrors.Errorf("document key unmarshal error in operation type %s: %w", e.OperationType, err)
		}
		operationType, namespace, documentKey = "delete", *originalNs, key
	case "c":
		var selector oplogRsChangeEventV2CommandSelector
		if err := bson.Unmarshal(e.Object, &selector); err != nil {
			return nil, xerrors.Errorf("selector unmarshal error in operation type %s: %w", e.OperationType, err)
		}
		switch selector.toType() {
		case oplogRsChangeEventV2CommandTypeCreate:
			operationType, namespace = "create", MakeNamespace(originalNs.Database, selector.Create)
		case oplogRsChangeEventV2CommandTypeRename:
			nsFrom := ParseNamespace(selector.RenameCollection)
			nsTo := ParseNamespace(selector.RenamedTo)
			operationType, namespace, toNamespace = "rename", *nsFrom, *nsTo
		case oplogRsChangeEventV2CommandTypeDrop:
			operationType, namespace = "drop", MakeNamespace(originalNs.Database, selector.Drop)
		case oplogRsChangeEventV2CommandTypeDropdatabase:
			// TODO(@kry127) what is the collection? This is database only
			operationType, namespace = "dropDatabase", MakeNamespace(originalNs.Database, "")
		default:
			log.Errorf("Command selector is not supported in operation type %s: %v. Change keyEvent: %v",
				e.OperationType, selector, e)
			return nil, nil
		}
	case "n":
		return newNoopChangeEvent(e.Timestamp), nil
	default:
		return nil, xerrors.Errorf("Unknown operation type %s. Change keyEvent: %v", e.OperationType, e)
	}
	// OK, construct event
	return &KeyChangeEvent{
		OperationType: operationType,
		DocumentKey:   documentKey,
		Namespace:     namespace,
		ToNamespace:   toNamespace,
		ClusterTime:   e.Timestamp,
	}, nil
}
