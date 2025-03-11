package serializer

import (
	"io"

	"github.com/transferria/transferria/pkg/abstract"
	"golang.org/x/xerrors"
)

type RawSerializerConfig struct {
	AddClosingNewLine bool
}

type rawSerializer struct {
	config *RawSerializerConfig
}

type rawStreamSerializer struct {
	serializer rawSerializer
	writer     io.Writer
}

func (s *rawSerializer) Serialize(item *abstract.ChangeItem) ([]byte, error) {
	if !item.IsMirror() {
		return nil, abstract.NewFatalError(xerrors.New("unexpected input, expect no converted raw data"))
	}
	data, err := abstract.GetRawMessageData(*item)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct raw message data: %w", err)
	}

	if s.config.AddClosingNewLine {
		data = append(data, byte('\n'))
	}
	return data, nil
}

func createDefaultRawSerializerConfig() *RawSerializerConfig {
	return &RawSerializerConfig{
		AddClosingNewLine: false,
	}
}

func (s *rawStreamSerializer) SetStream(ostream io.Writer) error {
	s.writer = ostream
	return nil
}

func (s *rawStreamSerializer) Serialize(items []*abstract.ChangeItem) error {
	for _, item := range items {
		data, err := s.serializer.Serialize(item)
		if err != nil {
			return xerrors.Errorf("rawStreamSerializer: failed to serialize item: %w", err)
		}
		_, err = s.writer.Write(data)
		if err != nil {
			return xerrors.Errorf("rawStreamSerializer: failed write serialized data: %w", err)
		}
	}
	return nil
}

func (s *rawStreamSerializer) Close() error {
	return nil
}

func NewRawSerializer(conf *RawSerializerConfig) *rawSerializer {
	if conf == nil {
		conf = createDefaultRawSerializerConfig()
	}

	return &rawSerializer{
		config: conf,
	}
}

func NewRawStreamSerializer(ostream io.Writer, conf *RawSerializerConfig) *rawStreamSerializer {
	if conf == nil {
		conf = createDefaultRawSerializerConfig()
	}
	if !conf.AddClosingNewLine {
		conf.AddClosingNewLine = true
	}

	return &rawStreamSerializer{
		serializer: *NewRawSerializer(conf),
		writer:     ostream,
	}
}
