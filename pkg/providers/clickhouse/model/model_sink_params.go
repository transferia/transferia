package model

import (
	"time"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

const (
	// BufferTriggingSizeDefault is a recommended default value for bufferer trigging size
	// Default value assume that we have 4 thread writer in 3gb box (default runtime box)
	// so each thread would consume at most 256 * 2 (one time for source one time for target) mb + some constant memory
	// in total it would eat 512 * 4 = 2gb, which is less than 3gb
	BufferTriggingSizeDefault uint64 = 256 * humanize.MiByte
)

//---
// ch

var _ ChSinkServerParams = (*ChDestinationWrapper)(nil)
var _ ChSinkServerParams = (*ChSourceWrapper)(nil)

type ChSinkServerParams interface {
	MdbClusterID() string
	ChClusterName() string
	User() string
	Password() string
	ResolvePassword() (string, error)
	Database() string
	// Partition
	// string, substitutes after 'PARTITION BY' in ddl. Field absent in UI.
	// 'ddl += fmt.Sprintf(" PARTITION BY (%v)", t.config.Partition)'
	Partition() string
	// Host
	// filled by SinkCluster for SinkServer.
	// the only field, which is absent in the model.
	Host() *clickhouse.Host
	PemFileContent() string
	SSLEnabled() bool
	// TTL
	// string, substitutes after 'TTL' in ddl. Field absent in UI. Nobody used.
	// example: '_timestamp + INTERVAL 18 MONTH'
	TTL() string
	// IsUpdateable
	// automatically derived from transfer options.
	// Updateable - data-transfer term, means the table satisfies two conditions:
	//     1) ReplacingMergeTree engine family
	//     2) table contains data-transfer system columns: '__data_transfer_commit_time', '__data_transfer_delete_time'
	IsUpdateable() bool

	// UpsertAbsentToastedRows When batch push fails on TOAST, interpret as sequential independent upserts.
	// Useful in cases:
	//  1. YDB Source with 'Updates' changefeed mode
	//  2. Any IncrementOnly transfer in ClickHouse which can bring update for inexistent document (for instance PG->CH)
	UpsertAbsentToastedRows() bool
	InferSchema() bool // If table exists - get it schema
	// MigrationOptions
	// Sink table modification settings
	MigrationOptions() ChSinkMigrationOptions
	GetIsSchemaMigrationDisabled() bool
	// UploadAsJSON enables JSON format upload. See CH destination model for details.
	UploadAsJSON() bool
	// AnyAsString
	// it's used only when UploadAsJSON=true.
	// for non-date/time & string types - when true, made one more json.Marshal. Why?
	AnyAsString() bool
	// SystemColumnsFirst
	// it seems we can derive it - just like we derive 'IsUpdateable' flag.
	// furthermore - we can get rid of 'system' columns term - just merge it with 'key' columns
	SystemColumnsFirst() bool
	Cleanup() model.CleanupType
	RootCertPaths() []string
	InsertSettings() InsertParams
	GetConnectionID() string
}

type ChSinkMigrationOptions struct {
	// AddNewColumns
	// automatically alter table to add new columns
	AddNewColumns bool
}

type ChSinkServerParamsWrapper struct {
	Model *ChSinkServerParams
}

func (w ChSinkServerParamsWrapper) GetConnectionID() string {
	return (*w.Model).GetConnectionID()
}

var _ ChSinkClusterParams = (*ChDestinationWrapper)(nil)
var _ ChSinkClusterParams = (*ChSourceWrapper)(nil)

type ChSinkClusterParams interface {
	ChSinkServerParams
	// AltHosts
	// In the model it calls 'Hosts'
	//
	// https://github.com/ClickHouse/clickhouse-go#dsn
	// alt_hosts - comma-separated list of single address hosts for load-balancing
	// We can get it from user, and we can fill it from mdb dbaas.
	//
	// for every AltHost, sinkCluster has special sinkServer
	//
	// it's very ad-hoc field - every sinker rewrites it as it want
	AltHosts() []*clickhouse.Host

	// ShardByTransferID
	// TODO(@timmyb32r) - is it meaningful?) highly likely something wrong with this option.
	// see: TM-2060 - it's for sharded pg
	// it's close to TM-2517, but we need to add sharding-by-src-shard on ch-sink
	// after TM-2517 we can remove this field & describe best-practice of dealing with sharded data
	ShardByTransferID() bool // another sharding option. TODO - why it's location differs form ShardCol
	ShardByRoundRobin() bool

	// technical needs

	MakeChildServerParams(hosts *clickhouse.Host) ChSinkServerParams
}

type ChSinkClusterParamsWrapper struct {
	Model *ChSinkClusterParams
}

func (w ChSinkClusterParamsWrapper) GetConnectionID() string {
	return (*w.Model).GetConnectionID()
}

var _ ChSinkShardParams = (*ChDestinationWrapper)(nil)
var _ ChSinkShardParams = (*ChSourceWrapper)(nil)

type ChSinkShardParams interface {
	ChSinkClusterParams
	// UseSchemaInTableName
	// add schema to tableName. TODO - replace it by universal transformer
	UseSchemaInTableName() bool
	// ShardCol
	// column_name, which is used for sharding
	// Meaningful only for queue-sources! bcs there are one 'column' and big amount of data.
	// For replication-dst we automatically turning-off sharding!
	ShardCol() string
	// Interval returns the minimum interval between two subsequent Pushes
	Interval() time.Duration
	// Tables
	// it's 'AltNames'. TODO - replace it by universal transformer
	Tables() map[string]string
}

type ChSinkShardParamsWrapper struct {
	Model *ChSinkShardParams
}

func (w ChSinkShardParamsWrapper) GetConnectionID() string {
	return (*w.Model).GetConnectionID()
}

var _ ChSinkParams = (*ChDestinationWrapper)(nil)
var _ ChSinkParams = (*ChSourceWrapper)(nil)

type ChSinkParams interface {
	ChSinkShardParams
	// Rotation
	// TODO - I think we don't need this (bcs of TTL in schema), and if need - we can make it by some universal mechanism
	Rotation() *model.RotatorConfig

	Shards() map[string][]*clickhouse.Host // shardName->[host]. It's used in sink.go to slice on shards

	// ColumnToShardIndex returns a user-provided exact mapping of shard key to shard name
	ColumnToShardName() map[string]string

	// technical needs

	MakeChildShardParams(altHosts []*clickhouse.Host) ChSinkShardParams
	SetShards(shards map[string][]*clickhouse.Host)
}

type ChSinkParamsWrapper struct {
	Model *ChSinkParams
}

func (w ChSinkParamsWrapper) GetConnectionID() string {
	return (*w.Model).GetConnectionID()
}

func (s *ChSource) ToSinkParams() (ChSourceWrapper, error) {
	copyChSource := *s
	connectionParams, err := ConnectionParamsFromSource(s)
	if err != nil {
		return ChSourceWrapper{}, err
	}
	result := ChSourceWrapper{
		Model:            &copyChSource,
		host:             connectionParams.Hosts[0],
		connectionParams: *connectionParams,
	}

	return result, nil
}
