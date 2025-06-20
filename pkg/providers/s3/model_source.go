//go:build !disable_s3_provider

package s3

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
)

func init() {
	gobwrapper.Register(new(S3Source))
	model.RegisterSource(ProviderType, func() model.Source {
		return new(S3Source)
	})
}

var _ model.Source = (*S3Source)(nil)

const (
	// defaultReadBatchSize is magic number by in-leskin, impacts how many rows we push each times
	// we need to push rather small chunks so our bufferer can buffer effectively
	defaultReadBatchSize = 128
	// defaultBlockSize impacts how many bytes we read fon each request from S3 bucket
	// its also used in replication as a mem limit to how many inflight bytes we can have.
	defaultBlockSize = 10_000_000
	// defaultInflightLimit impacts when to throttle async push in order to not OOM when push buffer becomes too big.
	defaultInflightLimit = 100_000_000
)

type UnparsedPolicy string

var (
	UnparsedPolicyFail     = UnparsedPolicy("fail")
	UnparsedPolicyContinue = UnparsedPolicy("continue")
	UnparsedPolicyRetry    = UnparsedPolicy("retry")
)

type S3Source struct {
	Bucket           string
	ConnectionConfig ConnectionConfig
	PathPrefix       string

	HideSystemCols bool // to hide system cols `__file_name` and `__row_index` cols from out struct
	ReadBatchSize  int
	InflightLimit  int64

	// s3 hold always single table, and TableID of such table defined by user
	TableName      string
	TableNamespace string

	InputFormat  model.ParsingFormat
	OutputSchema []abstract.ColSchema

	AirbyteFormat string // this is for backward compatibility with airbyte. we store raw format for later parsing.
	PathPattern   string

	Format         Format
	EventSource    EventSource
	UnparsedPolicy UnparsedPolicy

	// ShardingParams describes configuration of sharding logic.
	// 	When nil, each file is a separate table part.
	// 	When enabled, each part grows depending on configuration.
	ShardingParams *ShardingParams

	// Concurrency - amount of parallel goroutines into one worker on REPLICATION
	Concurrency            int64
	SyntheticPartitionsNum int
}

// TODO: Add sharding of one file to bytes ranges.
type ShardingParams struct {
	// PartBytesLimit limits total files sizes (in bytes) per part.
	// NOTE: It could be exceeded, but not more than the size of last file in part.
	PartBytesLimit uint64
	PartFilesLimit uint64 // PartFilesLimit limits total files count per part.
}

type ConnectionConfig struct {
	AccessKey        string
	S3ForcePathStyle bool
	SecretKey        model.SecretString
	Endpoint         string
	UseSSL           bool
	VerifySSL        bool
	Region           string
	ServiceAccountID string
}

type EventSource struct {
	SQS    *SQS
	SNS    *SNS
	PubSub *PubSub
}

type ProtoSetting struct {
	DescFile         []byte
	DescResourceName string
	MessageName      string

	IncludeColumns []protoparser.ColParams
	PrimaryKeys    []string
	PackageType    protoparser.MessagePackageType

	NullKeysAllowed bool
}

type Format struct {
	CSVSetting     *CSVSetting
	JSONLSetting   *JSONLSetting
	ParquetSetting *ParquetSetting
	ProtoParser    *ProtoSetting
}

type (
	SQS struct {
		QueueName        string
		OwnerAccountID   string
		ConnectionConfig ConnectionConfig
	}
	SNS    struct{} // Will be implemented in ORION-3447
	PubSub struct{} // Will be implemented in ORION-3448
)

type (
	CSVSetting struct {
		Delimiter               string
		QuoteChar               string
		EscapeChar              string
		Encoding                string
		DoubleQuote             bool
		NewlinesInValue         bool
		BlockSize               int64
		AdditionalReaderOptions AdditionalOptions
		AdvancedOptions         AdvancedOptions
	}
	JSONLSetting struct {
		NewlinesInValue         bool
		BlockSize               int64
		UnexpectedFieldBehavior UnexpectedFieldBehavior
	}
	ParquetSetting struct{}
)

type AdditionalOptions struct {
	// auto_dict_encode and auto_dict_max_cardinality check_utf8 are currently skipped for simplicity reasons

	NullValues             []string `json:"null_values,omitempty"`
	TrueValues             []string `json:"true_values,omitempty"`
	FalseValues            []string `json:"false_values,omitempty"`
	DecimalPoint           string   `json:"decimal_point,omitempty"`
	StringsCanBeNull       bool     `json:"strings_can_be_null,omitempty"`        // default false
	QuotedStringsCanBeNull bool     `json:"quoted_strings_can_be_null,omitempty"` // default true
	IncludeColumns         []string `json:"include_columns,omitempty"`
	IncludeMissingColumns  bool     `json:"include_missing_columns,omitempty"` // default false
	TimestampParsers       []string `json:"timestamp_parsers,omitempty"`
}

type AdvancedOptions struct {
	// bloc_size, use_threads and encoding are currently skipped for simplicity and handled separately

	SkipRows                int64    `json:"skip_rows,omitempty"`
	SkipRowsAfterNames      int64    `json:"skip_rows_after_names,omitempty"`
	ColumnNames             []string `json:"column_names,omitempty"`
	AutogenerateColumnNames bool     `json:"autogenerate_column_names,omitempty"` // default true
}

type UnexpectedFieldBehavior int

const (
	Unspecified UnexpectedFieldBehavior = iota
	Infer
	Ignore
	Error
)

func (s *S3Source) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *S3Source) Validate() error {
	return nil
}

func (s *S3Source) WithDefaults() {
	if s.ReadBatchSize == 0 {
		s.ReadBatchSize = defaultReadBatchSize
	}
	if s.InflightLimit == 0 {
		s.InflightLimit = defaultInflightLimit
	}
	if s.Concurrency == 0 {
		s.Concurrency = 10
	}
	if s.SyntheticPartitionsNum == 0 {
		s.SyntheticPartitionsNum = 128
	}
	s.ConnectionConfig.S3ForcePathStyle = true

	if s.InputFormat == model.ParsingFormatJSONLine {
		if s.Format.JSONLSetting == nil {
			s.Format.JSONLSetting = new(JSONLSetting)
		}
		if s.Format.JSONLSetting.UnexpectedFieldBehavior == 0 {
			s.Format.JSONLSetting.UnexpectedFieldBehavior = Infer
		}
		if s.Format.JSONLSetting.BlockSize == 0 {
			s.Format.JSONLSetting.BlockSize = defaultBlockSize
		}
	}

	if s.InputFormat == model.ParsingFormatCSV {
		if s.Format.CSVSetting == nil {
			s.Format.CSVSetting = new(CSVSetting)
		}

		if s.Format.CSVSetting.Delimiter == "" {
			s.Format.CSVSetting.Delimiter = ","
		}
		if s.Format.CSVSetting.BlockSize == 0 {
			s.Format.CSVSetting.BlockSize = defaultBlockSize
		}
	}
}

func (s *S3Source) IsAppendOnly() bool {
	return true
}

func (s *S3Source) IsSource() {}

func (s *S3Source) IsAbstract2(model.Destination) bool { return len(s.AirbyteFormat) > 0 } // for airbyte legacy format compatibility

func (s *S3Source) TableID() abstract.TableID {
	return abstract.TableID{Namespace: s.TableNamespace, Name: s.TableName}
}
