package model

import (
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
)

var (
	//go:embed doc_destination_usage.md
	destinationUsage []byte
	//go:embed doc_destination_example.yaml
	destinationExample []byte
)

type ClickHouseColumnValueToShardName struct {
	ColumnValue string
	ShardName   string
}

var (
	_ model.Destination = (*ChDestination)(nil)
	_ model.Describable = (*ChDestination)(nil)
)

// ChDestination - see description of fields in sink_params.go
type ChDestination struct {
	// ChSinkServerParams
	MdbClusterID     string `json:"Cluster"`
	ChClusterName    string // CH cluster to which data will be transfered. Other clusters would be ignored.
	User             string
	Password         model.SecretString
	Database         string
	Partition        string
	SSLEnabled       bool
	HTTPPort         int
	NativePort       int
	TTL              string
	InferSchema      bool
	MigrationOptions *ChSinkMigrationOptions
	// ForceJSONMode forces JSON protocol at sink:
	// - allows upload records without 'required'-fields, clickhouse fills them via defaults.
	//         BUT IF THEY ARE 'REQUIRED' - WHAT THE POINT?
	// - allows new data types
	// - allows composite data types
	// - allows schemas with expressions & defaults
	// - allows handle some 'alias'
	// - json-protocol is slower than native
	//
	// JSON protocol implementation currently only supports InsertKind items.
	// This option used to be public.
	ForceJSONMode           bool `json:"ForceHTTP"`
	ProtocolUnspecified     bool // Denotes that the original proto configuration does not specify the protocol
	AnyAsString             bool
	SystemColumnsFirst      bool
	IsUpdateable            bool
	UpsertAbsentToastedRows bool

	// Insert settings
	InsertParams InsertParams

	// AltHosts
	Hosts []string

	// ChSinkShardParams
	UseSchemaInTableName bool
	ShardCol             string
	Interval             time.Duration
	AltNamesList         []model.AltName

	// ChSinkParams
	ShardByTransferID          bool
	ShardByRoundRobin          bool
	Rotation                   *model.RotatorConfig
	ShardsList                 []ClickHouseShard
	ColumnValueToShardNameList []ClickHouseColumnValueToShardName

	// fields used only in wrapper-over-sink
	TransformerConfig  map[string]string
	SubNetworkID       string
	SecurityGroupIDs   []string
	Cleanup            model.CleanupType
	PemFileContent     string // timmyb32r: this field is not used in sinker! It seems we are not able to transfer into on-premise ch with cert
	InflightBuffer     int    // deprecated: use BufferTriggingSize instead. Items' count triggering a buffer flush
	BufferTriggingSize uint64
	RootCACertPaths    []string
}

type InsertParams struct {
	MaterializedViewsIgnoreErrors bool
}

func (p InsertParams) AsQueryPart() string {
	settingsQ := ""
	var settings []string
	if p.MaterializedViewsIgnoreErrors {
		settings = append(settings, "materialized_views_ignore_errors = '1'")
	}
	if len(settings) > 0 {
		settingsQ = fmt.Sprintf("SETTINGS %s", strings.Join(settings, ","))
	}
	return settingsQ
}

func (d *ChDestination) Describe() model.Doc {
	return model.Doc{
		Usage:   string(destinationUsage),
		Example: string(destinationExample),
	}
}

func (d *ChDestination) MDBClusterID() string {
	return d.MdbClusterID
}

func (d *ChDestination) ClusterID() string {
	return d.MdbClusterID
}

func (d *ChDestination) Transformer() map[string]string {
	return d.TransformerConfig
}

func (d *ChDestination) CleanupMode() model.CleanupType {
	return d.Cleanup
}

func (d *ChDestination) WithDefaults() {
	if d.Interval == 0 {
		d.Interval = time.Second
	}
	if d.Cleanup == "" {
		d.Cleanup = model.Drop
	}
	if d.NativePort == 0 {
		d.NativePort = 9440
	}
	if d.HTTPPort == 0 {
		d.HTTPPort = 8443
	}

	if d.BufferTriggingSize == 0 {
		d.BufferTriggingSize = BufferTriggingSizeDefault
	}
	if d.MigrationOptions == nil {
		d.MigrationOptions = &ChSinkMigrationOptions{
			AddNewColumns: true,
		}
	}
}

func (d *ChDestination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    d.InflightBuffer,
		TriggingSize:     d.BufferTriggingSize,
		TriggingInterval: d.Interval,
	}
}

func (ChDestination) IsDestination() {}

func (d *ChDestination) GetProviderType() abstract.ProviderType {
	return "ch"
}

func (d *ChDestination) Shards() map[string][]string {
	shardsMap := map[string][]string{}
	for _, shard := range d.ShardsList {
		shardsMap[shard.Name] = shard.Hosts
	}
	return shardsMap
}

func (d *ChDestination) Validate() error {
	d.Rotation = d.Rotation.NilWorkaround()
	if err := d.Rotation.Validate(); err != nil {
		return err
	}
	if len(d.ColumnValueToShardNameList) > 0 && len(d.ShardsList) > 0 {
		shards := d.Shards()
		for _, columnValueToShardName := range d.ColumnValueToShardNameList {
			if _, ok := shards[columnValueToShardName.ShardName]; !ok {
				return xerrors.Errorf("Invalid shard name for value mapping: %s -> %s: no such shard",
					columnValueToShardName.ColumnValue, columnValueToShardName.ShardName)
			}
		}
	}
	return nil
}

func (d *ChDestination) shallUseJSON(transfer *model.Transfer) bool {
	if d.ForceJSONMode || !d.ProtocolUnspecified {
		return d.ForceJSONMode
	}
	if transfer.Type == abstract.TransferTypeSnapshotOnly {
		return true
	}
	// kostyl while HTTP pusher writes bytes as base64 strings
	if transfer.Src != nil && transfer.Src.GetProviderType() == "metrika" {
		return false
	}
	return model.IsAppendOnlySource(transfer.Src)
}

// ToSinkParams converts the model into sink properties object, which contains extra information which depends on transfer type
func (d *ChDestination) ToSinkParams(transfer *model.Transfer) ChDestinationWrapper {
	wrapper := newChDestinationWrapper(*d)
	wrapper.useJSON = d.shallUseJSON(transfer)
	return *wrapper
}

// ToReplicationFromPGSinkParams converts the model into sink properties object that would be constructed for a replication from PostgreSQL
func (d *ChDestination) ToReplicationFromPGSinkParams() ChDestinationWrapper {
	return *newChDestinationWrapper(*d)
}

func (d *ChDestination) FillDependentFields(transfer *model.Transfer) {
	if !model.IsAppendOnlySource(transfer.Src) && !transfer.SnapshotOnly() {
		d.IsUpdateable = true
		if d.ShardCol != "" {
			d.ShardCol = ""
			logger.Log.Warn("turned off sharding on ch-dst, sharding is allowed only for queue-src")
		}
	}
}

// ChDestinationWrapper implements ChSinkParams
type ChDestinationWrapper struct {
	Model *ChDestination
	host  string // host is here, bcs it needed only in SinkServer/SinkTable
	// useJSON is calculated in runtime, not by the model
	useJSON       bool
	migrationOpts ChSinkMigrationOptions
}

func (d ChDestinationWrapper) InsertSettings() InsertParams {
	return d.Model.InsertParams
}

// newChDestinationWrapper copies the model provided to it in order to be able to modify the fields in it
func newChDestinationWrapper(model ChDestination) *ChDestinationWrapper {
	return &ChDestinationWrapper{
		Model:         &model,
		host:          "",
		useJSON:       false,
		migrationOpts: *model.MigrationOptions,
	}
}

func (d ChDestinationWrapper) RootCertPaths() []string {
	return d.Model.RootCACertPaths
}

func (d ChDestinationWrapper) Cleanup() model.CleanupType {
	return d.Model.Cleanup
}

func (d ChDestinationWrapper) MdbClusterID() string {
	return d.Model.MdbClusterID
}

func (d ChDestinationWrapper) ChClusterName() string {
	return d.Model.ChClusterName
}

func (d ChDestinationWrapper) User() string {
	return d.Model.User
}

func (d ChDestinationWrapper) Password() string {
	return string(d.Model.Password)
}

func (d ChDestinationWrapper) ResolvePassword() (string, error) {
	password, err := ResolvePassword(d.MdbClusterID(), d.User(), string(d.Model.Password))
	return password, err
}

func (d ChDestinationWrapper) Database() string {
	return d.Model.Database
}

func (d ChDestinationWrapper) Partition() string {
	return d.Model.Partition
}

func (d ChDestinationWrapper) Host() *string {
	return &d.host
}

func (d ChDestinationWrapper) SSLEnabled() bool {
	return d.Model.SSLEnabled || d.MdbClusterID() != ""
}

func (d ChDestinationWrapper) HTTPPort() int {
	return d.Model.HTTPPort
}

func (d ChDestinationWrapper) NativePort() int {
	return d.Model.NativePort
}

func (d ChDestinationWrapper) TTL() string {
	return d.Model.TTL
}

func (d ChDestinationWrapper) IsUpdateable() bool {
	return d.Model.IsUpdateable
}

func (d ChDestinationWrapper) UpsertAbsentToastedRows() bool {
	return d.Model.UpsertAbsentToastedRows
}

func (d ChDestinationWrapper) InferSchema() bool {
	return d.Model.InferSchema
}

func (d ChDestinationWrapper) MigrationOptions() ChSinkMigrationOptions {
	return d.migrationOpts
}

func (d ChDestinationWrapper) UploadAsJSON() bool {
	return d.useJSON
}

func (d ChDestinationWrapper) AnyAsString() bool {
	return d.Model.AnyAsString
}

func (d ChDestinationWrapper) SystemColumnsFirst() bool {
	return d.Model.SystemColumnsFirst
}

func (d ChDestinationWrapper) AltHosts() []string {
	return d.Model.Hosts
}

func (d ChDestinationWrapper) UseSchemaInTableName() bool {
	return d.Model.UseSchemaInTableName
}

func (d ChDestinationWrapper) ShardCol() string {
	return d.Model.ShardCol
}

func (d ChDestinationWrapper) Interval() time.Duration {
	return d.Model.Interval
}

func (d ChDestinationWrapper) Tables() map[string]string {
	altNamesMap := map[string]string{}
	for _, altName := range d.Model.AltNamesList {
		altNamesMap[altName.From] = altName.To
	}
	return altNamesMap
}

func (d ChDestinationWrapper) ShardByTransferID() bool {
	return d.Model.ShardByTransferID
}

func (d ChDestinationWrapper) ShardByRoundRobin() bool {
	return d.Model.ShardByRoundRobin
}

func (d ChDestinationWrapper) Rotation() *model.RotatorConfig {
	return d.Model.Rotation
}

func (d ChDestinationWrapper) Shards() map[string][]string {
	return d.Model.Shards()
}

func (d ChDestinationWrapper) ColumnToShardName() map[string]string {
	columnValueToShardNameMap := map[string]string{}
	for _, ColumnValueToShardName := range d.Model.ColumnValueToShardNameList {
		columnValueToShardNameMap[ColumnValueToShardName.ColumnValue] = ColumnValueToShardName.ShardName
	}
	return columnValueToShardNameMap
}

func (d ChDestinationWrapper) PemFileContent() string {
	return d.Model.PemFileContent
}

func (d ChDestinationWrapper) MakeChildServerParams(host string) ChSinkServerParams {
	newChDestination := *d.Model
	newChDestinationWrapper := ChDestinationWrapper{
		Model:         &newChDestination,
		host:          host,
		useJSON:       d.useJSON,
		migrationOpts: d.MigrationOptions(),
	}
	return newChDestinationWrapper
}

func (d ChDestinationWrapper) MakeChildShardParams(altHosts []string) ChSinkShardParams {
	newChDestination := *d.Model
	newChDestinationWrapper := ChDestinationWrapper{
		Model:         &newChDestination,
		host:          "",
		useJSON:       d.useJSON,
		migrationOpts: d.MigrationOptions(),
	}
	newChDestination.Hosts = altHosts
	return newChDestinationWrapper
}

// SetShards
// we can set model variables, bcs we make copy of ChDestination in NewChDestinationV1
func (d ChDestinationWrapper) SetShards(shards map[string][]string) {
	d.Model.ShardsList = []ClickHouseShard{}
	for shardName, hosts := range shards {
		d.Model.ShardsList = append(d.Model.ShardsList, ClickHouseShard{
			Name:  shardName,
			Hosts: hosts,
		})
	}
}
