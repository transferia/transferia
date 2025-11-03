package parameters

import "github.com/transferia/transferia/library/go/core/xerrors"

func Validate(connectorParameters map[string]string, dropKeys bool) error {
	dtBatchingMaxSize := GetBatchingMaxSize(connectorParameters)
	if dtBatchingMaxSize != 0 {
		if dropKeys {
			if GetValueConverterSchemaRegistryURL(connectorParameters) == "" {
				return xerrors.New("dt.batching.max.size can be used ONLY with schema-registry for values encoding")
			}
		} else {
			return xerrors.New("dt.batching.max.size can be used only with lb/yds")
		}
	}
	return nil
}
