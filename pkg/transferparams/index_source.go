package transferparams

import (
	"os"
	"strconv"
	"strings"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type IndexSource interface {
	fillParams(*Provider) error
}

var (
	_ IndexSource = (*InstanceNameIndexSource)(nil)
	_ IndexSource = (*EnvironmentVariableIndexSource)(nil)
	_ IndexSource = (*DummyIndexSource)(nil)
)

type InstanceNameIndexSource struct{}

type EnvironmentVariableIndexSource struct {
	VariableName string
}

type DummyIndexSource struct{}

func (*InstanceNameIndexSource) fillParams(params *Provider) error {
	name, err := params.instanceName()
	if err != nil {
		return xerrors.Errorf("cannot get instance name for worker index: %w", err)
	}

	index, err := calculateJobIndexFromInstanceName(name)
	if err != nil {
		// Preserve the existing IG behavior: malformed names produce
		// a warning and the index returned by the existing parser.
		logger.Log.Warn("Failed to obtain current job index", log.Error(err))
	}

	params.transferParams[workerIndex] = index
	return nil
}

func (s *EnvironmentVariableIndexSource) fillParams(params *Provider) error {
	if s.VariableName == "" {
		return xerrors.New("worker index environment variable name is empty")
	}

	value, ok := os.LookupEnv(s.VariableName)
	if !ok || value == "" {
		return xerrors.Errorf("worker index environment variable %q is not set or is empty", s.VariableName)
	}

	index, err := strconv.Atoi(value)
	if err != nil {
		return xerrors.Errorf("cannot parse worker index from environment variable %q: %w", s.VariableName, err)
	}

	params.transferParams[workerIndex] = index
	return nil
}

func (*DummyIndexSource) fillParams(params *Provider) error {
	params.transferParams[workerIndex] = 0
	return nil
}

func calculateJobIndexFromInstanceName(instanceName string) (int, error) {
	parts := strings.Split(instanceName, "-")
	if len(parts) == 1 {
		return 0, xerrors.Errorf("instance name is in unexpected format: %q", instanceName)
	}
	jobIDX := parts[len(parts)-1]
	idx, err := strconv.Atoi(jobIDX)
	if err != nil {
		return 0, xerrors.Errorf("fail to parse job index from instance name %q: %w", instanceName, err)
	}
	if idx < 1 {
		return 0, nil
	}
	return idx - 1, nil
}
