//go:build !disable_kafka_provider

package kafka

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	debezium_prod_status "github.com/transferia/transferia/pkg/debezium/prodstatus"
	"github.com/transferia/transferia/pkg/providers/airbyte"
	clickhouse "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

type Mirrareable interface {
	ForceMirror()
}

func InferFormatSettings(src model.Source, formatSettings model.SerializationFormat) model.SerializationFormat {
	result := formatSettings.Copy()

	if result.Name == model.SerializationFormatAuto {
		if model.IsDefaultMirrorSource(src) {
			result.Name = model.SerializationFormatMirror
			return *result
		}
		if model.IsAppendOnlySource(src) {
			result.Name = model.SerializationFormatJSON
			return *result
		}

		switch src.(type) {
		case Mirrareable:
			result.Name = model.SerializationFormatLbMirror
		case *airbyte.AirbyteSource:
			result.Name = model.SerializationFormatJSON
		case *clickhouse.ChSource:
			result.Name = model.SerializationFormatNative
		default:
			result.Name = model.SerializationFormatDebezium
		}
	}
	if result.Name == model.SerializationFormatDebezium {
		switch s := src.(type) {
		case *postgres.PgSource:
			if _, ok := result.Settings[debeziumparameters.DatabaseDBName]; !ok {
				result.Settings[debeziumparameters.DatabaseDBName] = s.Database
			}
			result.Settings[debeziumparameters.SourceType] = "pg"
		case *mysql.MysqlSource:
			result.Settings[debeziumparameters.SourceType] = "mysql"
		}
	}

	return *result
}

func sourceCompatible(src model.Source, transferType abstract.TransferType, serializationName model.SerializationFormatName) error {
	switch serializationName {
	case model.SerializationFormatAuto:
		return nil
	case model.SerializationFormatDebezium:
		if debezium_prod_status.IsSupportedSource(src.GetProviderType().Name(), transferType) {
			return nil
		}
		return xerrors.Errorf("in debezium serializer not supported source type: %s", src.GetProviderType().Name())
	case model.SerializationFormatJSON:
		if src.GetProviderType().Name() == airbyte.ProviderType.Name() {
			return nil
		}
		if model.IsAppendOnlySource(src) {
			return nil
		}
		return xerrors.New("in JSON serializer supported only next source types: AppendOnly and airbyte")
	case model.SerializationFormatNative:
		return nil
	case model.SerializationFormatMirror:
		if model.IsDefaultMirrorSource(src) {
			return nil
		}
		return xerrors.New("in Mirror serialized supported only default mirror source types")
	case model.SerializationFormatLbMirror:
		if src.GetProviderType().Name() == "lb" { // sorry again
			return nil
		}
		return xerrors.New("in LbMirror serialized supported only lb source type")
	case model.SerializationFormatRawColumn:
		return nil
	default:
		return xerrors.Errorf("unknown serializer name: %s", serializationName)
	}
}
