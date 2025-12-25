package ydb

import (
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	"github.com/transferia/transferia/pkg/debezium/typeutil"
	"github.com/transferia/transferia/pkg/util/jsonx"
)

//---------------------------------------------------------------------------------------------------------------------
// ydb non-default converting

var KafkaTypeToOriginalTypeToFieldReceiverFunc = map[debeziumcommon.KafkaType]map[string]debeziumcommon.FieldReceiver{
	debeziumcommon.KafkaTypeInt8: {
		"ydb:Uint8": new(debeziumcommon.Int8ToUint8Default),
	},
	debeziumcommon.KafkaTypeInt16: {
		"ydb:Uint16": new(debeziumcommon.Int16ToUint16Default),
	},
	debeziumcommon.KafkaTypeInt32: {
		"ydb:Uint32": new(debeziumcommon.IntToUint32Default),
		"ydb:Date":   new(Date),
	},
	debeziumcommon.KafkaTypeInt64: {
		"ydb:Uint64":    new(debeziumcommon.Int64ToUint64Default),
		"ydb:Datetime":  new(Datetime),
		"ydb:Timestamp": new(Timestamp),
		"ydb:Interval":  new(Interval),
	},
	debeziumcommon.KafkaTypeFloat32: {
		"ydb:Float": new(debeziumcommon.Float64ToFloat32Default),
	},
	debeziumcommon.KafkaTypeString: {
		"ydb:Json":         new(JSON),
		"ydb:JsonDocument": new(JSON),
	},
}

type Date struct {
	debeziumcommon.Int64ToTime
	debeziumcommon.YTTypeDate
	debeziumcommon.FieldReceiverMarker
}

func (i *Date) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (time.Time, error) {
	return typeutil.TimeFromDate(in), nil
}

type Datetime struct {
	debeziumcommon.Int64ToTime
	debeziumcommon.YTTypeDateTime
	debeziumcommon.FieldReceiverMarker
}

func (i *Datetime) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (time.Time, error) {
	return typeutil.TimeFromDatetime(in), nil
}

type Timestamp struct {
	debeziumcommon.Int64ToTime
	debeziumcommon.YTTypeTimestamp
	debeziumcommon.FieldReceiverMarker
}

func (i *Timestamp) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (time.Time, error) {
	return typeutil.TimeFromTimestamp(in), nil
}

type Interval struct {
	debeziumcommon.DurationToInt64
	debeziumcommon.YTTypeInterval
	debeziumcommon.FieldReceiverMarker
}

func (i *Interval) Do(in time.Duration, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (int64, error) {
	return int64(in), nil
}

type JSON struct {
	debeziumcommon.StringToAny
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (i *JSON) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (interface{}, error) {
	var result interface{}
	if err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&result); err != nil {
		return "", xerrors.Errorf("unable to unmarshal json - err: %w", err)
	}
	return result, nil
}
