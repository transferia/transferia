package transformer

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/terryid"
	"github.com/transferia/transferia/pkg/util"
)

type Config any

const ID = "transformerId"

type WithID interface {
	ID() string
}

// Transformer serializeable one-of wrapper, for backward compatibility
// will hold both type discriminator and config value itself, config is just an any object
// on later stages it materializes as real config object, specific to transformer
type Transformer map[abstract.TransformerType]interface{}

func (t Transformer) Type() abstract.TransformerType {
	for k := range t {
		if k == ID {
			continue
		}
		return abstract.TransformerType(util.Snakify(string(k)))
	}
	return ""
}

func (t Transformer) ID() string {
	id, ok := t[ID]
	if !ok {
		return terryid.GenerateTransformerID()
	}
	return id.(string)
}

func (t Transformer) Config() Config {
	for k, v := range t {
		if k == ID {
			continue
		}
		return v
	}
	return nil
}

type Transformers struct {
	DebugMode    bool          `json:"debugMode"`
	Transformers []Transformer `json:"transformers"`
	ErrorsOutput *ErrorsOutput `json:"errorsOutput"`
}

type OutputType string

const (
	SinkErrorsOutput    = OutputType("sink")
	DevnullErrorsOutput = OutputType("devnull")
)

type ErrorsOutput struct {
	Type   OutputType
	Config any
}
