package queue

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

func New(format model.SerializationFormat, saveTxOrder, dropKeys, isSnapshot bool, logger log.Logger) (Serializer, error) {
	var result Serializer
	var err error
	switch format.Name {
	case model.SerializationFormatDebezium:
		result, err = NewDebeziumSerializer(format.Settings, saveTxOrder, dropKeys, isSnapshot, logger)
	case model.SerializationFormatJSON:
		result, err = NewJSONSerializer(*format.BatchingSettings, saveTxOrder, logger)
	case model.SerializationFormatMirror, model.SerializationFormatLbMirror:
		result, err = NewMirrorSerializer(logger)
	case model.SerializationFormatNative:
		result, err = NewNativeSerializer(*format.BatchingSettings, saveTxOrder)
	case model.SerializationFormatRawColumn:
		result = NewRawColumnSerializer(format.Settings[model.ColumnNameParamName], logger)
	default:
		return nil, xerrors.Errorf("unknown serialization format: %s", format.Name)
	}
	if err != nil {
		return result, xerrors.Errorf("unable to create serializer: %w", err)
	}
	return result, nil
}
