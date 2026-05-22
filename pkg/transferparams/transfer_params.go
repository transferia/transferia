package transferparams

import (
	"os"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/googlemetadata"
	"github.com/transferia/transferia/pkg/util"
	"gopkg.in/yaml.v2"
)

var (
	provider *Provider
)

func Init(source Source) error {
	if provider != nil {
		return nil
	}
	switch paramsSourceConfig := source.(type) {
	case *VmMetadataSource:
		metadataProvider, err := newProviderFromMetadata()
		if err != nil {
			return xerrors.Errorf("cannot initialize metadata provider: %w", err)
		}
		provider = metadataProvider
	case *EnvironmentVariableSource:
		envVariableProvider, err := newProviderFronEnv(paramsSourceConfig.VariableName)
		if err != nil {
			return xerrors.Errorf("cannot initialize env variable provider: %w", err)
		}
		provider = envVariableProvider
	default:
		return xerrors.Errorf("unknown transfer params source type: %q", source)
	}
	return nil
}

func GetProvider() (*Provider, error) {
	if provider == nil {
		return nil, xerrors.New("transfer params provider is not initialized")
	}
	return provider, nil
}

func newProviderFromMetadata() (*Provider, error) {
	metadata, err := googlemetadata.GetMetadataStringField(googlemetadata.GoogleUserData)
	if err != nil {
		return nil, xerrors.Errorf("cannot get metadata: %w", err)
	}
	transferParams := map[transferParam]interface{}{}
	if err := yaml.Unmarshal([]byte(metadata), &transferParams); err != nil {
		return nil, xerrors.Errorf("cannot unmarshal transfer params from metadata: %w", err)
	}

	// next fields are required for correct work of transfer manager
	// will be fairly moved to separate field in future
	instanceNameVal, err := googlemetadata.GetMetadataStringField(googlemetadata.GoogleName)
	if err != nil {
		return nil, xerrors.Errorf("cannot get instance name from metadata: %w", err)
	}
	transferParams[instanceName] = instanceNameVal
	instanceIdVal, err := googlemetadata.GetMetadataStringField(googlemetadata.GoogleID)
	if err != nil {
		return nil, xerrors.Errorf("cannot get instance id from metadata: %w", err)
	}
	transferParams[instanceId] = instanceIdVal

	return &Provider{transferParams: transferParams}, nil
}

func newProviderFronEnv(envName string) (*Provider, error) {
	evnValue, ok := os.LookupEnv(envName)
	if !ok {
		return nil, xerrors.Errorf("Environment variable %s is not set", envName)
	}
	if evnValue == "" {
		return nil, xerrors.Errorf("Environment variable %s is empty", envName)
	}
	transferParams := map[transferParam]interface{}{}
	if err := yaml.Unmarshal([]byte(evnValue), &transferParams); err != nil {
		return nil, xerrors.Errorf("cannot unmarshal transfer params from environment variable %s: %w", envName, err)
	}

	transferIdVal, err := util.GetStringValue(transferParams, transferId)
	if err != nil {
		return nil, xerrors.Errorf("cannot get transfer id from environment variable %s: %w", envName, err)
	}
	// this hack is needed for correct work of transfer manager, because in some places this field uses for extract jobIndex
	// will be fairly moved to separate field in future
	transferParams[instanceName] = transferIdVal + "-1"

	return &Provider{transferParams: transferParams}, nil
}

type transferParam string

const (
	logLevel          = transferParam("log_level")
	useFineGrainedSSA = transferParam("usefinegrainedssa")
	spec              = transferParam("spec")
	instanceId        = transferParam("instance_id")
	instanceName      = transferParam("instance_name")
	cloudId           = transferParam("cloud_id")
	folderId          = transferParam("folder_id")
	runtimeFlavor     = transferParam("runtime_flavor")
	jobCount          = transferParam("job_count")
	transferId        = transferParam("transfer_id")
)

func (p transferParam) string() string {
	return string(p)
}
