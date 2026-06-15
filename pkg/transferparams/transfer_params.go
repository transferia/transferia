package transferparams

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
)

var (
	provider *Provider
)

func Init(source Source) error {
	if provider != nil {
		return nil
	}
	var err error
	provider, err = source.provider()
	if err != nil {
		return xerrors.Errorf("cannot initialize transfer params provider: %w", err)
	}
	return nil
}

func GetProvider() (*Provider, error) {
	if provider == nil {
		return nil, xerrors.New("transfer params provider is not initialized")
	}
	return provider, nil
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
