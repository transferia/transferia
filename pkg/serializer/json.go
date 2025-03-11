package serializer

import (
	"encoding/json"
	"io"

	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/xerrors"
)

type JSONSerializerConfig struct {
	UnsupportedItemKinds map[abstract.Kind]bool
	AddClosingNewLine    bool
	AnyAsString          bool
}

type jsonSerializer struct {
	config *JSONSerializerConfig
}

type jsonStreamSerializer struct {
	serializer jsonSerializer
	writer     io.Writer
}

func (s *jsonSerializer) Serialize(item *abstract.ChangeItem) ([]byte, error) {
	if !item.IsRowEvent() {
		return nil, nil
	}
	if s.config.UnsupportedItemKinds[item.Kind] {
		return nil, xerrors.Errorf("JsonSerializer: unsupported kind: %s", item.Kind)
	}

	kv := make(map[string]interface{}, len(item.ColumnNames))
	for i := range item.ColumnNames {
		if s.config.AnyAsString && item.TableSchema.Columns()[i].DataType == string(schema.TypeAny) && item.ColumnValues[i] != nil {
			valueData, err := json.Marshal(item.ColumnValues[i])
			if err != nil {
				return nil, xerrors.Errorf("JsonSerializer: unable to serialize kv map: %w", err)
			}
			kv[item.ColumnNames[i]] = string(valueData)
		} else {
			kv[item.ColumnNames[i]] = item.ColumnValues[i]
		}
	}

	data, err := json.Marshal(kv)
	if err != nil {
		return nil, xerrors.Errorf("JsonSerializer: unable to serialize kv map: %w", err)
	}

	if s.config.AddClosingNewLine {
		data = append(data, byte('\n'))
	}

	return data, nil
}

func (s *jsonStreamSerializer) Serialize(items []*abstract.ChangeItem) error {
	for _, item := range items {
		data, err := s.serializer.Serialize(item)
		if err != nil {
			return xerrors.Errorf("jsonStreamSerializer: failed to serialize item: %w", err)
		}
		_, err = s.writer.Write(data)
		if err != nil {
			return xerrors.Errorf("jsonStreamSerializer: failed write serialized data: %w", err)
		}
	}
	return nil
}

func (s *jsonStreamSerializer) Close() error {
	return nil
}

func createDefaultJSONSerializerConfig() *JSONSerializerConfig {
	return &JSONSerializerConfig{
		UnsupportedItemKinds: nil,
		AddClosingNewLine:    false,
		AnyAsString:          false,
	}
}

func NewJSONSerializer(conf *JSONSerializerConfig) *jsonSerializer {
	if conf == nil {
		conf = createDefaultJSONSerializerConfig()
	}

	return &jsonSerializer{
		config: conf,
	}
}

func NewJSONStreamSerializer(ostream io.Writer, conf *JSONSerializerConfig) *jsonStreamSerializer {
	if conf == nil {
		conf = createDefaultJSONSerializerConfig()
	}
	if !conf.AddClosingNewLine {
		conf.AddClosingNewLine = true
	}
	jsonSerializer := NewJSONSerializer(conf)
	return &jsonStreamSerializer{
		serializer: *jsonSerializer,
		writer:     ostream,
	}
}
