package model

import (
	"encoding/json"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
)

const (
	HourFormat  = "2006-01-02T15:04:05"
	DayFormat   = "2006-01-02"
	MonthFormat = "2006-01"
)

type (
	BytesSize    uint32
	SecretString string
)

type (
	BackupMode    string
	ParsingFormat string
	LineSplitter  string
)

const (
	LineSplitterNewLine    = LineSplitter("NEW_LINE")
	LineSplitterNoSplitter = LineSplitter("NO_SPLITTER")

	S3BackupModeErrorOnly    = BackupMode("ERROR_ONLY")
	S3BackupModeAllDocuments = BackupMode("ALL_DOCUMENTS")
	S3BackupModeNoBackup     = BackupMode("NO_BACKUP")

	ParsingFormatNative   = ParsingFormat("Native")
	ParsingFormatJSON     = ParsingFormat("JSON")
	ParsingFormatJSONLine = ParsingFormat("JSONL")
	ParsingFormatTSKV     = ParsingFormat("TSKV")
	ParsingFormatCSV      = ParsingFormat("CSV")
	ParsingFormatPROTO    = ParsingFormat("PROTO")
	ParsingFormatLine     = ParsingFormat("LINE")
	ParsingFormatORC      = ParsingFormat("ORC")
	ParsingFormatPARQUET  = ParsingFormat("PARQUET")
	ParsingFormatDebezium = ParsingFormat("Debezium")
	ParsingFormatRaw      = ParsingFormat("RAW")
)

type TLSMode string

const (
	DefaultTLS  TLSMode = "Default"
	EnabledTLS  TLSMode = "Enabled"
	DisabledTLS TLSMode = "Disabled"
)

type DataTransformOptions struct {
	CloudFunction         string
	ServiceAccountID      string
	NumberOfRetries       int64
	BufferSize            BytesSize
	BufferFlushInterval   time.Duration
	InvocationTimeout     time.Duration
	S3ErrorBucket         string
	BackupMode            BackupMode
	CloudFunctionsBaseURL string
	Headers               []HeaderValue
}

type HeaderValue struct {
	Key, Value string
}

type AltName struct {
	From string
	To   string
}

func createSourceBase(provider abstract.ProviderType) (Source, error) {
	fac, ok := knownSources[provider]
	if !ok {
		return nil, xerrors.Errorf("unknown source provider: %s", provider)
	}
	return fac(), nil
}

func NewSource(provider abstract.ProviderType, jsonStr string) (Source, error) {
	source, err := createSourceBase(provider)
	if err != nil {
		return nil, xerrors.Errorf("cannot create source base: %w", err)
	}
	if err := json.Unmarshal([]byte(jsonStr), source); err != nil {
		return nil, xerrors.Errorf("cannot unmarshal JSON: %w", err)
	}
	source.WithDefaults()
	return source, nil
}

func createDestinationBase(provider abstract.ProviderType) (Destination, error) {
	fac, ok := knownDestinations[provider]
	if !ok {
		return nil, xerrors.Errorf("unknown destination provider: %s", provider)
	}
	return fac(), nil
}

func NewDestination(provider abstract.ProviderType, jsonStr string) (Destination, error) {
	destination, err := createDestinationBase(provider)
	if err != nil {
		return nil, xerrors.Errorf("cannot create destination base: %w", err)
	}
	if serializable, ok := destination.(Serializeable); ok {
		if err := serializable.SetParams(jsonStr); err != nil {
			return nil, xerrors.Errorf("cannot unmarshal JSON (YT destination): %w", err)
		}
	} else {
		if err := json.Unmarshal([]byte(jsonStr), &destination); err != nil {
			return nil, xerrors.Errorf("cannot unmarshal JSON: %w", err)
		}
	}
	destination.WithDefaults()
	return destination, nil
}
