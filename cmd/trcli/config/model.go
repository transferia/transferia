package config

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/transformer"
	"gopkg.in/yaml.v3"
)

type Endpoint struct {
	Type   abstract.ProviderType
	Params any
}

func (e Endpoint) RawParams() string {
	switch p := e.Params.(type) {
	case []byte:
		return string(p)
	case string:
		return p
	default:
		data, _ := yaml.Marshal(p)
		return string(data)
	}
}

type Runtime struct {
	Type   string
	Params interface{}
}

type UploadTables struct {
	Tables []abstract.TableDescription
}

type TransferYamlView struct {
	ID                string
	TransferName      string
	Description       string
	Labels            string
	Status            model.TransferStatus
	Type              abstract.TransferType
	FolderID          string
	CloudID           string
	Src               Endpoint
	Dst               Endpoint
	RegularSnapshot   *abstract.RegularSnapshot `yaml:"regular_snapshot"`
	Transformation    *transformer.Transformers `yaml:"transformation"`
	DataObjects       *model.DataObjects        `yaml:"data_objects"`
	TypeSystemVersion int                       `yaml:"type_system_version"`
	AsyncOperations   bool
}

func (v TransferYamlView) Validate() error {
	if v.Transformation == nil || v.Transformation.Transformers == nil {
		return nil
	}
	var errs error
	for _, tr := range v.Transformation.Transformers {
		_, err := transformer.New(tr.Type(), tr.Config(), logger.Log, abstract.TransformationRuntimeOpts{JobIndex: 0})
		if err != nil {
			errs = multierr.Append(errs, xerrors.Errorf("unable to construct %s(%s): %w", tr.Type(), tr.ID(), err))
		}
	}
	if errs != nil {
		return xerrors.Errorf("transformers invalid: %w", errs)
	}
	return nil
}
