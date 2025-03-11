package kinesis

import (
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/parsers"
)

var (
	_ model.Source = (*KinesisSource)(nil)
)

type KinesisSource struct {
	Endpoint              string
	Region                string
	Stream                string
	BufferSize            int
	AccessKey             string
	SecretKey             model.SecretString
	ParserConfig          map[string]interface{}
	ParseQueueParallelism int
}

func (k *KinesisSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (k *KinesisSource) Validate() error {
	return nil
}

func (k *KinesisSource) WithDefaults() {
	if k.BufferSize == 0 {
		k.BufferSize = 128 * 1024 * 1024
	}
}

func (k *KinesisSource) IsSource() {}

func (k *KinesisSource) IsAppendOnly() bool {
	if k.ParserConfig == nil {
		return true
	} else {
		parserConfigStruct, _ := parsers.ParserConfigMapToStruct(k.ParserConfig)
		if parserConfigStruct == nil {
			return true
		}
		return parserConfigStruct.IsAppendOnly()
	}
}
