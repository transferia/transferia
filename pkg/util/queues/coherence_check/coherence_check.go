package coherence_check

import (
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	debezium_prod_status "github.com/transferia/transferia/pkg/debezium/prodstatus"
	"github.com/transferia/transferia/pkg/providers/airbyte"
	clickhouse "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"go.ytsaurus.tech/library/go/core/log"
)

func inferFormatSettings(src model.Source, formatSettings model.SerializationFormat) model.SerializationFormat {
	result := formatSettings.Copy()

	if result.Name == model.SerializationFormatAuto {
		if model.IsDefaultMirrorSource(src) {
			result.Name = model.SerializationFormatMirror
			return *result
		}
		if model.IsLbMirrorSource(src) {
			result.Name = model.SerializationFormatLbMirror
			return *result
		}
		if model.IsAppendOnlySource(src) {
			result.Name = model.SerializationFormatJSON
			return *result
		}
		if debezium_prod_status.IsSupportedSource(src.GetProviderType().Name(), abstract.TransferTypeNone) {
			result.Name = model.SerializationFormatDebezium
			return *result
		}

		switch src.(type) {
		case *airbyte.AirbyteSource:
			result.Name = model.SerializationFormatJSON
		case *clickhouse.ChSource:
			result.Name = model.SerializationFormatNative
		default:
			return model.SerializationFormat{
				Name:             "",
				Settings:         nil,
				SettingsKV:       nil,
				BatchingSettings: nil,
			}
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

func SourceCompatible(src model.Source, transferType abstract.TransferType, serializationName model.SerializationFormatName) error {
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
		if src.GetProviderType().Name() == "lb" || src.GetProviderType().Name() == "lf" || src.GetProviderType().Name() == "yds" { // sorry again
			return nil
		}
		return xerrors.New("in LbMirror serialized supported only lb source type")
	case model.SerializationFormatRawColumn:
		return nil
	default:
		return xerrors.Errorf("unknown serializer name: %s", serializationName)
	}
}

func InferFormatSettings(lgr log.Logger, src model.Source, formatSettings model.SerializationFormat) model.SerializationFormat {
	formatSettingsArr, _ := json.Marshal(formatSettings)
	lgr.Infof("InferFormatSettings - input - srcProviderName:%s, formatSettings:%s", src.GetProviderType().Name(), string(formatSettingsArr))
	result := inferFormatSettings(src, formatSettings)
	resultArr, _ := json.Marshal(result)
	lgr.Infof("InferFormatSettings - output:%s", string(resultArr))
	return result
}
