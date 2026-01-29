package serializer

import (
	"bytes"
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

func (s *jsonSerializer) SerializeWithSeparatorTo(item *abstract.ChangeItem, separator []byte, buf *bytes.Buffer) error {
	if !item.IsRowEvent() {
		return nil
	}
	if s.config.UnsupportedItemKinds[item.Kind] {
		return xerrors.Errorf("JsonSerializer: unsupported kind: %s", item.Kind)
	}

	kv := make(map[string]interface{}, len(item.ColumnNames))
	for i := range item.ColumnNames {
		columnName := item.ColumnNames[i]
		value := item.ColumnValues[i]

		finalValue := value
		if s.config.AnyAsString && item.TableSchema.Columns()[i].DataType == string(schema.TypeAny) && value != nil {
			valueData, err := json.Marshal(value)
			if err != nil {
				return xerrors.Errorf("JsonSerializer: unable to serialize kv map: %w", err)
			}
			finalValue = string(valueData)
		}

		kv[columnName] = finalValue
	}

	// Use encoder with SetEscapeHTML(false) to preserve original characters like &, <, >
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(kv); err != nil {
		return xerrors.Errorf("JsonSerializer: unable to serialize kv map: %w", err)
	}
	// Remove trailing newline added by Encode()
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' && !s.config.AddClosingNewLine {
		buf.Truncate(buf.Len() - 1)
	} else if s.config.AddClosingNewLine && (len(data) == 0 || data[len(data)-1] != '\n') {
		buf.WriteByte('\n')
	}

	if len(separator) > 0 {
		if _, err := buf.Write(separator); err != nil {
			return xerrors.Errorf("JsonSerializer: unable to write separator: %w", err)
		}
	}

	return nil
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
		columnName := item.ColumnNames[i]
		value := item.ColumnValues[i]

		var finalValue interface{}
		finalValue = value
		if s.config.AnyAsString && item.TableSchema.Columns()[i].DataType == string(schema.TypeAny) && value != nil {
			valueData, err := json.Marshal(value)
			if err != nil {
				return nil, xerrors.Errorf("JsonSerializer: unable to serialize kv map: %w", err)
			}
			finalValue = string(valueData)
		}

		kv[columnName] = finalValue
	}

	// Use encoder with SetEscapeHTML(false) to preserve original characters like &, <, >
	buf := new(bytes.Buffer)
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(kv); err != nil {
		return nil, xerrors.Errorf("JsonSerializer: unable to serialize kv map: %w", err)
	}
	// Remove trailing newline added by Encode()
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' && !s.config.AddClosingNewLine {
		data = data[:len(data)-1]
	} else if s.config.AddClosingNewLine && (len(data) == 0 || data[len(data)-1] != '\n') {
		data = append(data, byte('\n'))
	}

	return data, nil
}

func (s *jsonSerializer) Close() ([]byte, error) {
	return nil, nil
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
