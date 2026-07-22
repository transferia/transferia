package coherence_check

import (
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	debezium_prod_status "github.com/transferia/transferia/pkg/debezium/prodstatus"
	provider_airbyte "github.com/transferia/transferia/pkg/providers/airbyte"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
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
		case *provider_airbyte.AirbyteSource:
			result.Name = model.SerializationFormatJSON
			return nil
		case *clickhouse_model.ChSource:
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
		case *provider_postgres.PgSource:
			if _, ok := result.Settings[debezium_parameters.DatabaseDBName]; !ok {
				result.Settings[debezium_parameters.DatabaseDBName] = s.Database
			}
			result.Settings[debezium_parameters.SourceType] = "pg"
		case *provider_mysql.MysqlSource:
			result.Settings[debezium_parameters.SourceType] = "mysql"
		case *provider_ydb.YdbSource:
			result.Settings[debezium_parameters.SourceType] = "ydb"
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
		if src.GetProviderType().Name() == provider_airbyte.ProviderType.Name() {
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

func marshalSanitizedSerializationFormat(formatSettings model.SerializationFormat) (string, error) {
	fsSanitized := formatSettings.Copy()
	fsSanitized.SanitizeSecrets()

	fsMarshalled, err := json.Marshal(fsSanitized)
	if err != nil {
		return "", xerrors.Errorf("unable to marshal sanitizedformat settings: %w", err)
	}
	return string(fsMarshalled), nil
}

func InferFormatSettings(lgr log.Logger, src model.Source, formatSettings model.SerializationFormat) (model.SerializationFormat, error) {
	formatSettingsBefore, err := marshalSanitizedSerializationFormat(formatSettings)
	lgr.Info("InferFormatSettings - input", log.String("src_provider_name", src.GetProviderType().Name()), log.String("format_settings", formatSettingsBefore), log.Error(err))
	result, err := inferFormatSettings(src, formatSettings)
	if err != nil {
		return emptyObject, xerrors.Errorf("unable to infer format settings: %w", err)
	}
	formatSettingsAfter, err := marshalSanitizedSerializationFormat(result)
	lgr.Info("InferFormatSettings - output", log.String("src_provider_name", src.GetProviderType().Name()), log.String("format_settings", formatSettingsAfter), log.Error(err))
	return result, nil
}
