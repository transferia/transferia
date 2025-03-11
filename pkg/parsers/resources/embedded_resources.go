package resources

import (
	"github.com/transferria/transferria/library/go/core/resource"
	"github.com/transferria/transferria/library/go/core/xerrors"
)

type EmbeddedResources struct {
	resNameToContent map[string]string
}

func (r *EmbeddedResources) GetResource(resName string) (string, error) {
	if content, ok := r.resNameToContent[resName]; ok {
		return content, nil
	} else {
		return "", xerrors.Errorf("resource %s not found", resName)
	}
}

func (r *EmbeddedResources) RunWatcher() {
}

func (r *EmbeddedResources) OutdatedCh() <-chan struct{} {
	return nil
}

func (r *EmbeddedResources) Close() {
}

func NewEmbeddedResources(resourceNames []string) (*EmbeddedResources, error) {
	resNameToContent := make(map[string]string)

	for _, name := range resourceNames {
		content := resource.Get(name)
		if content == nil {
			return nil, xerrors.Errorf("embedded resource %s not found", name)
		}
		resNameToContent[name] = string(content)
	}

	return &EmbeddedResources{
		resNameToContent: resNameToContent,
	}, nil
}
