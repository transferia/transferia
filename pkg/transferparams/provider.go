package transferparams

import (
	"github.com/transferia/transferia/pkg/util"
)

type Provider struct {
	transferParams map[transferParam]interface{}
}

func (p *Provider) LogLevel() (string, error) {
	return util.GetStringValue(p.transferParams, logLevel)
}

func (p *Provider) UseFineGrainedSSA() (bool, error) {
	return util.GetBoolValue(p.transferParams, useFineGrainedSSA)
}

func (p *Provider) Spec() (string, error) {
	return util.GetStringValue(p.transferParams, spec)
}

func (p *Provider) InstanceId() (string, error) {
	return util.GetStringValue(p.transferParams, instanceId)
}

func (p *Provider) InstanceName() (string, error) {
	return util.GetStringValue(p.transferParams, instanceName)
}

func (p *Provider) CloudId() (string, error) {
	return util.GetStringValue(p.transferParams, cloudId)
}

func (p *Provider) FolderId() (string, error) {
	return util.GetStringValue(p.transferParams, folderId)
}

func (p *Provider) RuntimeFlavor() (string, error) {
	return util.GetStringValue(p.transferParams, runtimeFlavor)
}

func (p *Provider) JobCount() (int, error) {
	return util.GetIntValue(p.transferParams, jobCount)
}
