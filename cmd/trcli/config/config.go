package config

import (
	"encoding/json"
	"os"
	"reflect"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/transformer"
	"gopkg.in/yaml.v3"
	sig_yaml "sigs.k8s.io/yaml"
)

func TransferFromYaml(params *string) (*model.Transfer, error) {
	// Don't override BASE_DIR if it's already set
	if _, ok := os.LookupEnv("BASE_DIR"); !ok {
		dir, err := os.MkdirTemp("", "shared-volume")
		if err != nil {
			return nil, xerrors.Errorf("unable to create base dir: %w", err)
		}
		_ = os.Setenv("BASE_DIR", dir)
	}

	transferRaw, err := os.ReadFile(*params)
	if err != nil {
		return nil, xerrors.Errorf("unable to read yaml config file: %w", err)
	}

	transfer, err := ParseTransfer(transferRaw)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse yaml config to transfer: %w", err)
	}
	return transfer, nil
}

func ParseTransfer(yaml []byte) (*model.Transfer, error) {
	tr, err := ParseTransferYaml(yaml)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse yaml: %w", err)
	}

	source, err := source(tr)
	if err != nil {
		return nil, xerrors.Errorf("failed to construct source: %w", err)
	}
	target, err := target(tr)
	if err != nil {
		return nil, xerrors.Errorf("failed to construct target: %w", err)
	}

	transfer := transfer(source, target, tr)

	transfer.FillDependentFields()
	if tr.Transformation != nil && len(tr.Transformation.Transformers) > 0 {
		transfer.Transformation = &model.Transformation{
			Transformers:      tr.Transformation,
			ExtraTransformers: nil,
			Executor:          nil,
			RuntimeJobIndex:   0,
		}
	}
	return transfer, nil
}

func StringToDurationHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		// Check if the source is a string and the target is time.Duration
		if f.Kind() == reflect.String && t == reflect.TypeOf(time.Duration(0)) {
			return time.ParseDuration(data.(string))
		}
		return data, nil
	}
}

func fieldsMismatch(params []byte, dummy model.EndpointParams) ([]byte, []string, []string, error) {
	foomap := make(map[string]interface{})
	err := json.Unmarshal(params, &foomap)
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("failed to remap model: %w", err)
	}

	// create a mapstructure decoder
	var md mapstructure.Metadata
	decoder, err := mapstructure.NewDecoder(
		&mapstructure.DecoderConfig{
			Metadata:   &md,
			Result:     &dummy,
			TagName:    "json",
			DecodeHook: mapstructure.ComposeDecodeHookFunc(StringToDurationHookFunc()),
		})
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("failed to prepare decoder: %w", err)
	}

	// decode the unmarshalled map into the given struct
	if err := decoder.Decode(foomap); err != nil {
		return nil, nil, nil, xerrors.Errorf("failed to decode: %w", err)
	}

	raw, err := json.Marshal(dummy)
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("unable to marshal struct to json: %w", err)
	}

	return raw, md.Unused, md.Unset, nil
}

// substituteEnv recursively iterates over an interface{} (which might be a string,
// a map, or a slice) and applies os.ExpandEnv to all string values.
func substituteEnv(val interface{}) interface{} {
	switch v := val.(type) {
	case string:
		return os.ExpandEnv(v)
	case map[string]interface{}:
		for key, inner := range v {
			v[key] = substituteEnv(inner)
		}
		return v
	case []interface{}:
		for i, inner := range v {
			v[i] = substituteEnv(inner)
		}
		return v
	default:
		return v
	}
}

func ParseTransferYaml(rawData []byte) (*TransferYamlView, error) {
	var transfer TransferYamlView
	if err := yaml.Unmarshal(rawData, &transfer); err != nil {
		return nil, err
	}

	var srcParams map[string]interface{}
	if err := yaml.Unmarshal([]byte(transfer.Src.RawParams()), &srcParams); err != nil {
		return nil, xerrors.Errorf("unable to parse source params: %w", err)
	}
	transfer.Src.Params = substituteEnv(srcParams).(map[string]interface{})

	var dstParams map[string]interface{}
	if err := yaml.Unmarshal([]byte(transfer.Dst.RawParams()), &dstParams); err != nil {
		return nil, xerrors.Errorf("unable to parse destination params: %w", err)
	}
	transfer.Dst.Params = substituteEnv(dstParams).(map[string]interface{})

	res, err := sig_yaml.YAMLToJSON([]byte(transfer.Src.RawParams()))
	if err == nil {
		transfer.Src.Params = string(res)
	}
	res, err = sig_yaml.YAMLToJSON([]byte(transfer.Dst.RawParams()))
	if err == nil {
		transfer.Dst.Params = string(res)
	}
	if transfer.Transformation != nil {
		for _, tr := range transfer.Transformation.Transformers {
			for k, v := range tr {
				tr[k] = convertMap(v)
			}
		}
	}
	return &transfer, nil
}

func convertMap(input interface{}) interface{} {
	switch value := input.(type) {
	case map[interface{}]interface{}:
		newMap := make(map[string]interface{})
		for k, v := range value {
			if key, ok := k.(string); ok {
				newMap[key] = convertMap(v)
			}
		}
		return newMap
	case []interface{}:
		for i, v := range value {
			value[i] = convertMap(v)
		}
	}
	return input
}

func TablesFromYaml(tablesParams *string) (*UploadTables, error) {
	rawData, err := os.ReadFile(*tablesParams)
	if err != nil {
		return nil, xerrors.Errorf("unable to read tables config file: %w", err)
	}
	tables, err := ParseTablesYaml(rawData)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse tables config file: %w", err)
	}
	return tables, nil
}

func ParseTablesYaml(rawData []byte) (*UploadTables, error) {
	var tables UploadTables
	if err := yaml.Unmarshal(rawData, &tables); err != nil {
		return nil, err
	}
	return &tables, nil
}

func source(tr *TransferYamlView) (model.Source, error) {
	dummy, err := model.NewSource(tr.Src.Type, "{}")
	if err != nil {
		return nil, xerrors.Errorf("unable to init empty model: %s: %w", tr.Src.Type, err)
	}
	rawJSON, unused, unset, err := fieldsMismatch([]byte(tr.Src.RawParams()), dummy)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct missed fields: %w", err)
	}
	if len(unused) > 0 {
		logger.Log.Infof("config for: %s source has %v unused fields", tr.Src.Type, unused)
	}
	if len(unset) > 0 {
		logger.Log.Infof("config for: %s source has %v unset fields", tr.Src.Type, unset)
	}

	return model.NewSource(tr.Src.Type, string(rawJSON))
}

func target(tr *TransferYamlView) (model.Destination, error) {
	dummy, err := model.NewDestination(tr.Dst.Type, "{}")
	if err != nil {
		return nil, xerrors.Errorf("unable to init empty model: %s: %w", tr.Dst.Type, err)
	}
	rawJSON, unused, unset, err := fieldsMismatch([]byte(tr.Dst.RawParams()), dummy)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct missed fields: %w", err)
	}
	if len(unused) > 0 {
		logger.Log.Infof("config for: %s destination has %v unused fields", tr.Dst.Type, unused)
	}
	if len(unset) > 0 {
		logger.Log.Infof("config for: %s destination has %v unset fields", tr.Dst.Type, unset)
	}
	return model.NewDestination(tr.Dst.Type, string(rawJSON))
}

func transfer(source model.Source, target model.Destination, tr *TransferYamlView) *model.Transfer {
	rt := new(abstract.LocalRuntime)
	rt.WithDefaults()
	transfer := new(model.Transfer)
	transfer.ID = tr.ID
	transfer.TransferName = tr.TransferName
	transfer.Description = tr.Description
	transfer.Labels = tr.Labels
	transfer.Status = model.New
	transfer.Type = tr.Type
	transfer.FolderID = tr.FolderID
	transfer.Runtime = rt
	transfer.Src = source
	transfer.Dst = target
	transfer.CloudID = tr.CloudID
	transfer.RegularSnapshot = tr.RegularSnapshot
	transfer.DataObjects = tr.DataObjects
	transfer.TypeSystemVersion = tr.TypeSystemVersion
	transfer.AsyncOperations = tr.AsyncOperations
	return transfer
}

func NewYamlView(tr *model.Transfer) *TransferYamlView {
	var transformations *transformer.Transformers
	if tr.Transformation != nil {
		transformations = tr.Transformation.Transformers
	}
	return &TransferYamlView{
		ID:                tr.ID,
		TransferName:      tr.TransferName,
		Description:       tr.Description,
		Labels:            tr.LabelsRaw(),
		Status:            tr.Status,
		Type:              tr.Type,
		FolderID:          tr.FolderID,
		CloudID:           tr.CloudID,
		Src:               Endpoint{Type: tr.SrcType(), Params: tr.Src},
		Dst:               Endpoint{Type: tr.DstType(), Params: tr.Dst},
		RegularSnapshot:   tr.RegularSnapshot,
		Transformation:    transformations,
		DataObjects:       tr.DataObjects,
		TypeSystemVersion: tr.TypeSystemVersion,
	}
}
