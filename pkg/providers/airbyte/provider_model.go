package airbyte

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/runtime/shared/pod"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
)

const (
	ProviderType = abstract.ProviderType("airbyte")
)

func init() {
	gobwrapper.RegisterName("*server.AirbyteSource", new(AirbyteSource))
	gobwrapper.RegisterName("*server.SyncStreams", new(SyncStreams))
	gobwrapper.RegisterName("*server.AirbyteStream", new(AirbyteStream))
	gobwrapper.RegisterName("*server.AirbyteSyncStream", new(AirbyteSyncStream))
	gobwrapper.RegisterName("*server.JSONSchema", new(JSONSchema))
	gobwrapper.RegisterName("*server.JSONProperty", new(JSONProperty))
	gobwrapper.RegisterName("*server.StringOrArray", new(StringOrArray))
	model.RegisterSource(ProviderType, func() model.Source {
		return new(AirbyteSource)
	})
	abstract.RegisterProviderName(ProviderType, "Airbyte")
}

type AirbyteSource struct {
	Config         string
	BaseDir        string
	ProtoConfig    string
	BatchSizeLimit model.BytesSize
	RecordsLimit   int
	EndpointType   EndpointType
	MaxRowSize     int
	Image          string
}

var _ model.Source = (*AirbyteSource)(nil)

type SyncStreams struct {
	Streams []AirbyteSyncStream `json:"streams"`
}

type AirbyteSyncStream struct {
	Stream              AirbyteStream `json:"stream"`
	SyncMode            string        `json:"sync_mode"`
	CursorField         []string      `json:"cursor_field"`
	DestinationSyncMode string        `json:"destination_sync_mode"`
}

func (s AirbyteSyncStream) TableID() abstract.TableID {
	return abstract.TableID{
		Namespace: s.Stream.Namespace,
		Name:      s.Stream.Name,
	}
}

type AirbyteStream struct {
	Name                    string     `json:"name"`
	JSONSchema              JSONSchema `json:"json_schema"`
	SupportedSyncModes      []string   `json:"supported_sync_modes"`
	DefaultCursorField      []string   `json:"default_cursor_field"`
	SourceDefinedPrimaryKey [][]string `json:"source_defined_primary_key"`
	Namespace               string     `json:"namespace"`
}

func (s AirbyteStream) TableID() abstract.TableID {
	return abstract.TableID{
		Namespace: s.Namespace,
		Name:      s.Name,
	}
}

type JSONSchema struct {
	Type       StringOrArray           `json:"type"`
	Properties map[string]JSONProperty `json:"properties"`
}

type manyTypes struct {
	Type        []string `json:"type"`
	Format      string   `json:"format"`
	AirbyteType string   `json:"airbyte_type"`
}
type oneType struct {
	Type        string `json:"type"`
	Format      string `json:"format"`
	AirbyteType string `json:"airbyte_type"`
}

type StringOrArray struct {
	Array []string
}

func (b *StringOrArray) MarshalJSON() ([]byte, error) {
	if len(b.Array) == 1 {
		return []byte(fmt.Sprintf(`"%s"`, b.Array[0])), nil
	}
	return json.Marshal(b.Array)
}

func (b *StringOrArray) UnmarshalJSON(data []byte) error {
	var errs util.Errors
	var many []string
	if err := json.Unmarshal(data, &many); err == nil {
		b.Array = many
		return nil
	} else {
		errs = append(errs, err)
	}
	var one string
	if err := json.Unmarshal(data, &one); err == nil {
		b.Array = []string{one}
		return nil
	} else {
		errs = append(errs, err)
	}
	return xerrors.Errorf("unable to parse json property: %w", errs)
}

func (b *JSONProperty) UnmarshalJSON(data []byte) error {
	var errs util.Errors
	var many manyTypes
	if err := json.Unmarshal(data, &many); err == nil {
		b.Type = many.Type
		b.AirbyteType = many.AirbyteType
		b.Format = many.Format
		return nil
	} else {
		errs = append(errs, err)
	}
	var one oneType
	if err := json.Unmarshal(data, &one); err == nil {
		b.Type = []string{one.Type}
		b.AirbyteType = one.AirbyteType
		b.Format = one.Format
		return nil
	} else {
		errs = append(errs, err)
	}
	return xerrors.Errorf("unable to parse json property: %w", errs)
}

// JSONProperty type can be array or single string, so had to implement custom unmarshal
type JSONProperty struct {
	Type        []string `json:"type"`
	Format      string   `json:"format"`
	AirbyteType string   `json:"airbyte_type"`
}

func (s *AirbyteSource) WithDefaults() {
	if s.MaxRowSize == 0 {
		s.MaxRowSize = 256 * 1024 * 1024 // 256mb large enough to handle everything, yet will not lead to OOM
	}
	if s.BatchSizeLimit == 0 {
		s.BatchSizeLimit = 10 * 1024 * 1024
	}
	if s.RecordsLimit == 0 {
		s.RecordsLimit = 10000
	}
	if s.ProtoConfig == "" {
		s.ProtoConfig = "{}"
	}
}

func (s *AirbyteSource) DataDir() string {
	if baseDir, ok := os.LookupEnv("BASE_DIR"); ok {
		return baseDir
	}
	if s.BaseDir == "" {
		return pod.SharedDir
	}
	return s.BaseDir
}

func (s *AirbyteSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *AirbyteSource) DockerImage() string {
	if s.Image != "" {
		return s.Image
	}
	return DefaultImages[s.EndpointType]
}

func (s *AirbyteSource) Validate() error {
	if s.Image != "" {
		return nil
	}
	if _, ok := DefaultImages[s.EndpointType]; !ok {
		return xerrors.Errorf("airbyte docker image not found for endpoint: %v", s.EndpointType)
	}
	return nil
}

func (*AirbyteSource) IsSource()                          {}
func (*AirbyteSource) IsStrictSource()                    {}
func (*AirbyteSource) IsAbstract2(model.Destination) bool { return false }
func (*AirbyteSource) IsIncremental()                     {}
func (*AirbyteSource) SupportsStartCursorValue() bool     { return false }

func (s *AirbyteSource) SupportMultiWorkers() bool {
	return false
}

func (s *AirbyteSource) SupportMultiThreads() bool {
	return false
}
