package airbyte

import (
	"encoding/json"
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type Stream struct {
	Name                    string          `json:"name"`
	JSONSchema              json.RawMessage `json:"json_schema"`
	SupportedSyncModes      []string        `json:"supported_sync_modes"`
	SourceDefinedCursor     bool            `json:"source_defined_cursor,omitempty"`
	DefaultCursorField      []string        `json:"default_cursor_field,omitempty"`
	SourceDefinedPrimaryKey [][]string      `json:"source_defined_primary_key,omitempty"`
	Namespace               string          `json:"namespace,omitempty"`
}

func (s *Stream) Validate() error {
	if len(s.SupportedSyncModes) == 0 {
		return fmt.Errorf("stream must have at least one supported_sync_modes")
	}
	return nil
}

func (s *Stream) TableID() abstract.TableID {
	return abstract.TableID{Namespace: s.Namespace, Name: s.Name}
}

func (s *Stream) ParsedJSONSchema() JSONSchema {
	var res JSONSchema
	_ = json.Unmarshal(s.JSONSchema, &res)
	return res
}

func (s *Stream) AsModel() *AirbyteStream {
	return &AirbyteStream{
		Name:                    s.Name,
		JSONSchema:              s.ParsedJSONSchema(),
		SupportedSyncModes:      s.SupportedSyncModes,
		DefaultCursorField:      s.DefaultCursorField,
		SourceDefinedPrimaryKey: s.SourceDefinedPrimaryKey,
		Namespace:               s.Namespace,
	}
}

func (s *Stream) SupportMode(mode string) bool {
	for _, supportedMode := range s.SupportedSyncModes {
		if supportedMode == mode {
			return true
		}
	}
	return false
}

func (s *Stream) Fqtn() string {
	return fmt.Sprintf(`"%s"."%s"`, s.Namespace, s.Name)
}

type DestinationSyncMode string

const (
	DestinationSyncModeAppend      DestinationSyncMode = "append"
	DestinationSyncModeOverwrite   DestinationSyncMode = "overwrite"
	DestinationSyncModeAppendDedup DestinationSyncMode = "append_dedup"
)

var AllDestinationSyncModes = []DestinationSyncMode{
	DestinationSyncModeAppend,
	DestinationSyncModeOverwrite,
	DestinationSyncModeAppendDedup,
}

func FindStream(streams []ConfiguredStream, obj string) (*ConfiguredStream, error) {
	tid, err := abstract.ParseTableID(obj)
	if err != nil {
		return nil, xerrors.Errorf("parse error: %s: %w", obj, err)
	}
	for _, stream := range streams {
		if *tid == stream.Stream.TableID() {
			return &stream, nil
		}
	}
	return nil, xerrors.Errorf("%s not found", obj)
}

type ConfiguredStream struct {
	Stream              Stream              `json:"stream"`
	SyncMode            string              `json:"sync_mode"`
	DestinationSyncMode DestinationSyncMode `json:"destination_sync_mode"`
	CursorField         []string            `json:"cursor_field,omitempty"`
	PrimaryKey          [][]string          `json:"primary_key,omitempty"`
}

func (c *ConfiguredStream) Validate() error {
	var err = c.Stream.Validate()
	if err != nil {
		return fmt.Errorf("stream invalid: %w", err)
	}
	var syncTypeValid = false
	for _, m := range c.Stream.SupportedSyncModes {
		if m == c.SyncMode {
			syncTypeValid = true
		}
	}
	if !syncTypeValid {
		return fmt.Errorf("unsupported syncMode: %s", c.SyncMode)
	}
	return nil
}

type Catalog struct {
	Streams []Stream `json:"streams"`
}

type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
}

func (c *ConfiguredCatalog) Validate() error {
	if len(c.Streams) == 0 {
		return fmt.Errorf("catalog must have at least one stream")
	}
	for i, s := range c.Streams {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("streams[%d]: %w", i, err)
		}
	}
	return nil
}

type Status string

const (
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
)

type ConnectionStatus struct {
	Status  Status `json:"status"`
	Message string `json:"message"`
}

type Record struct {
	Stream     string                 `json:"stream"`
	Data       json.RawMessage        `json:"data"`
	EmittedAt  int64                  `json:"emitted_at"`
	Namespace  string                 `json:"namespace,omitempty"`
	ParsedData map[string]interface{} `json:"-"`
}

func (r *Record) LazyParse() error {
	if r.ParsedData != nil {
		return nil
	}
	return json.Unmarshal(r.Data, &r.ParsedData)
}

type LogLevel string

const (
	LogLevelTrace LogLevel = "TRACE"
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
	LogLevelFatal LogLevel = "FATAL"
)

type Log struct {
	Level   LogLevel `json:"level"`
	Message string   `json:"message"`
}

type State struct {
	// Data is the actual state associated with the ingestion. This must be a JSON _Object_ in order
	// to comply with the airbyte specification.
	Data json.RawMessage `json:"data"`
	// Merge indicates that Data is an RFC 7396 JSON Merge Patch, and should
	// be be reduced into the previous state accordingly.
	Merge bool `json:"estuary.dev/merge,omitempty"`
}

func (s *State) UnmarshalJSON(b []byte) error {
	var tmp = struct {
		Data    json.RawMessage `json:"data"`
		NSMerge bool            `json:"estuary.dev/merge"`
		Merge   bool            `json:"merge"`
	}{}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}

	s.Data = tmp.Data
	s.Merge = tmp.NSMerge || tmp.Merge

	return nil
}

type Spec struct {
	DocumentationURL        string          `json:"documentationUrl,omitempty"`
	ChangelogURL            string          `json:"changelogUrl,omitempty"`
	ConnectionSpecification json.RawMessage `json:"connectionSpecification"`
	SupportsIncremental     bool            `json:"supportsIncremental,omitempty"`

	// SupportedDestinationSyncModes is ignored by Flow
	SupportedDestinationSyncModes []DestinationSyncMode `json:"supported_destination_sync_modes,omitempty"`
	// SupportsNormalization is not currently used or supported by Flow or estuary-developed
	// connectors
	SupportsNormalization bool `json:"supportsNormalization,omitempty"`
	// SupportsDBT is not currently used or supported by Flow or estuary-developed
	// connectors
	SupportsDBT bool `json:"supportsDBT,omitempty"`
	// AuthSpecification is not currently used or supported by Flow or estuary-developed
	// connectors
	AuthSpecification json.RawMessage `json:"authSpecification,omitempty"`
}

type MessageType string

const (
	MessageTypeRecord           MessageType = "RECORD"
	MessageTypeState            MessageType = "STATE"
	MessageTypeLog              MessageType = "LOG"
	MessageTypeTrace            MessageType = "TRACE"
	MessageTypeSpec             MessageType = "SPEC"
	MessageTypeConnectionStatus MessageType = "CONNECTION_STATUS"
	MessageTypeCatalog          MessageType = "CATALOG"
)

type Message struct {
	Type             MessageType       `json:"type"`
	Log              *Log              `json:"log,omitempty"`
	State            *State            `json:"state,omitempty"`
	Record           *Record           `json:"record,omitempty"`
	ConnectionStatus *ConnectionStatus `json:"connectionStatus,omitempty"`
	Spec             *Spec             `json:"spec,omitempty"`
	Catalog          *Catalog          `json:"catalog,omitempty"`
}
