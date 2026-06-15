package transferparams

import (
	"os"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/googlemetadata"
	"gopkg.in/yaml.v2"
)

type Source interface {
	provider() (*Provider, error)
}

var (
	_ Source = (*VmMetadataSource)(nil)
	_ Source = (*EnvironmentVariableSource)(nil)
	_ Source = (*FileSource)(nil)
)

type VmMetadataSource struct{}

type EnvironmentVariableSource struct {
	VariableName string
}
type FileSource struct {
	FilePath string
}

type DummySource struct {
}

func (*VmMetadataSource) provider() (*Provider, error) {
	metadata, err := googlemetadata.GetMetadataStringField(googlemetadata.GoogleUserData)
	if err != nil {
		return nil, xerrors.Errorf("cannot get metadata: %w", err)
	}
	transferParams := map[transferParam]any{}
	if err := yaml.Unmarshal([]byte(metadata), &transferParams); err != nil {
		return nil, xerrors.Errorf("cannot unmarshal transfer params from metadata: %w", err)
	}

	// next fields are required for correct work of data transfer
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

func (s *EnvironmentVariableSource) provider() (*Provider, error) {
	evnValue, ok := os.LookupEnv(s.VariableName)
	if !ok {
		return nil, xerrors.Errorf("Environment variable %s is not set", s.VariableName)
	}
	if evnValue == "" {
		return nil, xerrors.Errorf("Environment variable %s is empty", s.VariableName)
	}
	transferParams := map[transferParam]any{}
	if err := yaml.Unmarshal([]byte(evnValue), &transferParams); err != nil {
		return nil, xerrors.Errorf("cannot unmarshal transfer params from environment variable %s: %w", s.VariableName, err)
	}

	return &Provider{transferParams: transferParams}, nil
}

func (s *FileSource) provider() (*Provider, error) {
	file, err := os.Open(s.FilePath)
	if err != nil {
		return nil, xerrors.Errorf("cannot open file %s: %w", s.FilePath, err)
	}
	defer file.Close()
	transferParams := map[transferParam]any{}
	if err := yaml.NewDecoder(file).Decode(&transferParams); err != nil {
		return nil, xerrors.Errorf("cannot unmarshal transfer params from file %s: %w", s.FilePath, err)
	}

	return &Provider{transferParams: transferParams}, nil
}

func (s *DummySource) provider() (*Provider, error) {
	return &Provider{transferParams: map[transferParam]any{}}, nil
}
