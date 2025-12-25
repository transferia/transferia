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

var emptyObject model.SerializationFormat

func inferFormatSettings(src model.Source, formatSettings model.SerializationFormat) (model.SerializationFormat, error) {
	result := formatSettings.Copy()

	handleAuto := func(inOut *model.SerializationFormat) error {
		if model.IsDefaultMirrorSource(src) {
			inOut.Name = model.SerializationFormatMirror
			return nil
		}
		if model.IsLbMirrorSource(src) {
			inOut.Name = model.SerializationFormatLbMirror
			return nil
		}
		if model.IsAppendOnlySource(src) {
			inOut.Name = model.SerializationFormatJSON
			return nil
		}
		if debezium_prod_status.IsSupportedSource(src.GetProviderType().Name(), abstract.TransferTypeNone) {
			inOut.Name = model.SerializationFormatDebezium
			return nil
		}

		switch src.(type) {
		case *airbyte.AirbyteSource:
			result.Name = model.SerializationFormatJSON
			return nil
		case *clickhouse.ChSource:
			result.Name = model.SerializationFormatNative
			return nil
		default:
			*inOut = emptyObject
			return xerrors.Errorf("unsupported source type for AUTO serializer: %T", src)
		}
	}

	if result.Name == model.SerializationFormatAuto {
		err := handleAuto(result)
		if err != nil {
			return emptyObject, xerrors.Errorf("unable to handle 'auto' serialization format, err: %w", err)
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

	return *result, nil
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

func InferFormatSettings(lgr log.Logger, src model.Source, formatSettings model.SerializationFormat) (model.SerializationFormat, error) {
	formatSettingsArr, _ := json.Marshal(formatSettings)
	lgr.Infof("InferFormatSettings - input - srcProviderName:%s, formatSettings:%s", src.GetProviderType().Name(), string(formatSettingsArr))
	result, err := inferFormatSettings(src, formatSettings)
	if err != nil {
		return emptyObject, xerrors.Errorf("unable to infer format settings: %w", err)
	}
	resultArr, _ := json.Marshal(result)
	lgr.Infof("InferFormatSettings - output:%s", string(resultArr))
	return result, nil
}
