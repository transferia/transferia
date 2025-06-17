//go:build !disable_yt_provider

package yt

import (
	"encoding/json"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/config/env"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/maps"
)

const (
	dynamicDefaultChunkSize uint32 = 90_000            // items
	staticDefaultChunkSize         = 100 * 1024 * 1024 // bytes
	poolDefault                    = "transfer_manager"
)

type YtDestinationModel interface {
	dp_model.TmpPolicyProvider
	ytclient.ConnParams
	bufferer.Bufferable

	ToStorageParams() *YtStorageParams

	Path() string
	Cluster() string
	Token() string
	PushWal() bool
	NeedArchive() bool
	CellBundle() string
	TTL() int64
	OptimizeFor() string
	CanAlter() bool
	IsSchemaMigrationDisabled() bool
	TimeShardCount() int
	Index() []string
	HashColumn() string
	PrimaryMedium() string
	Pool() string
	Atomicity() yt.Atomicity
	LoseDataOnError() bool
	DiscardBigValues() bool
	TabletCount() int
	Rotation() *dp_model.RotatorConfig
	VersionColumn() string
	AutoFlushPeriod() int
	Ordered() bool
	UseStaticTableOnSnapshot() bool
	AltNames() map[string]string
	Spec() *YTSpec
	TolerateKeyChanges() bool
	InitialTabletCount() uint32
	WriteTimeoutSec() uint32
	ChunkSize() uint32
	BufferTriggingSize() uint64
	BufferTriggingInterval() time.Duration
	Transformer() map[string]string
	CleanupMode() dp_model.CleanupType
	WithDefaults()
	IsDestination()
	GetProviderType() abstract.ProviderType
	GetTableAltName(table string) string
	Validate() error
	SetSnapshotLoad()
	LegacyModel() interface{}
	AllowAlter()
	SetStaticTable()
	SetIndex(index []string)
	SetOrdered()
	CompressionCodec() yt.ClientCompressionCodec

	Static() bool
	SortedStatic() bool
	StaticChunkSize() int

	DisableDatetimeHack() bool // TODO(@kry127) when remove hack?

	GetConnectionData() ConnectionData
	DisableProxyDiscovery() bool

	SupportSharding() bool

	CustomAttributes() map[string]any
	// MergeAttributes should be used to merge user-defined custom table attributes
	// with arbitrary attribute set (usually table settings like medium, ttl, ...)
	// with the priority to the latter one
	// It guarantees to keep unchanged both the argument and custom attributes map in the model
	MergeAttributes(tableSettings map[string]any) map[string]any
}

type YtDestination struct {
	Path                      string
	Cluster                   string
	Token                     string
	PushWal                   bool
	NeedArchive               bool
	CellBundle                string
	TTL                       int64 // it's in milliseconds
	OptimizeFor               string
	CanAlter                  bool
	IsSchemaMigrationDisabled bool
	TimeShardCount            int
	Index                     []string
	HashColumn                string
	PrimaryMedium             string
	Pool                      string       // pool for running merge and sort operations for static tables
	Strict                    bool         // DEPRECATED, UNUSED IN NEW DATA PLANE - use LoseDataOnError and Atomicity
	Atomicity                 yt.Atomicity // Atomicity for the dynamic tables being created in YT. See https://yt.yandex-team.ru/docs/description/dynamic_tables/sorted_dynamic_tables#atomarnost

	// If true, some errors on data insertion to YT will be skipped, and a warning will be written to the log.
	// Among such errors are:
	// * we were unable to find table schema in cache for some reason: https://github.com/transferia/transferia/arcadia/transfer_manager/go/pkg/providers/yt/sink/sink.go?rev=11063561#L482-484
	// * a row (or a value inside a row) being inserted into the YT table has exceeded YT limits (16 MB by default).
	LoseDataOnError bool

	DiscardBigValues         bool
	TabletCount              int // DEPRECATED - remove in March
	Rotation                 *dp_model.RotatorConfig
	VersionColumn            string
	AutoFlushPeriod          int
	Ordered                  bool
	TransformerConfig        map[string]string
	UseStaticTableOnSnapshot bool // optional.Optional[bool] breaks compatibility
	AltNames                 map[string]string
	Cleanup                  dp_model.CleanupType
	Spec                     YTSpec
	TolerateKeyChanges       bool
	InitialTabletCount       uint32
	WriteTimeoutSec          uint32
	ChunkSize                uint32 // ChunkSize defines the number of items in a single request to YT for dynamic sink and chunk size in bytes for static sink
	BufferTriggingSize       uint64
	BufferTriggingInterval   time.Duration
	CompressionCodec         yt.ClientCompressionCodec
	DisableDatetimeHack      bool // This disable old hack for inverting time.Time columns as int64 timestamp for LF>YT
	Connection               ConnectionData
	CustomAttributes         map[string]string

	Static          bool
	SortedStatic    bool // true, if we need to sort static tables
	StaticChunkSize int  // desired size of static table chunk in bytes
}

func (d *YtDestination) GetUseStaticTableOnSnapshot() bool {
	return d.UseStaticTableOnSnapshot
}

type YtDestinationWrapper struct {
	Model *YtDestination
	// This is for pre/post-snapshot hacks (to be removed)
	_pushWal bool
}

var (
	_ dp_model.Destination          = (*YtDestinationWrapper)(nil)
	_ dp_model.AlterableDestination = (*YtDestinationWrapper)(nil)
)

func (d *YtDestinationWrapper) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Model)
}

func (d *YtDestinationWrapper) UnmarshalJSON(data []byte) error {
	var dest YtDestination
	if err := json.Unmarshal(data, &dest); err != nil {
		return xerrors.Errorf("unable to unmarshal yt destination: %w", err)
	}
	d.Model = &dest
	return nil
}

func (d *YtDestinationWrapper) IsAlterable() {}

func (d *YtDestinationWrapper) Params() string {
	r, _ := json.Marshal(d.Model)
	return string(r)
}

func (d *YtDestinationWrapper) SetParams(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), &d.Model)
}

// TODO: Remove in march
func (d *YtDestinationWrapper) DisableDatetimeHack() bool {
	return d.Model.DisableDatetimeHack
}

func (d *YtDestinationWrapper) EnsureTmpPolicySupported() error {
	if d.Static() {
		return xerrors.Errorf("static destination is not supported")
	}
	if d.UseStaticTableOnSnapshot() {
		return xerrors.Errorf("using static tables on snapshot is not supported")
	}
	return nil
}

func (d *YtDestinationWrapper) EnsureCustomTmpPolicySupported() error {
	if !d.UseStaticTableOnSnapshot() {
		return xerrors.New("using static tables on snapshot is not enabled")
	}
	return nil
}

func (d *YtDestinationWrapper) CompressionCodec() yt.ClientCompressionCodec {
	return d.Model.CompressionCodec
}

func (d *YtDestinationWrapper) SetOrdered() {
	d.Model.Ordered = true
}

func (d *YtDestinationWrapper) SetIndex(index []string) {
	d.Model.Index = index
}

func (d *YtDestinationWrapper) SetStaticTable() {
	d.Model.Static = true
}

func (d *YtDestinationWrapper) AllowAlter() {
	d.Model.CanAlter = true
	d.Model.IsSchemaMigrationDisabled = false
}

func (d *YtDestinationWrapper) PreSnapshotHacks() {
	d.SetSnapshotLoad()
}

func (d *YtDestinationWrapper) PostSnapshotHacks() {
	d.Model.PushWal = d._pushWal
}

func (d *YtDestinationWrapper) SetSnapshotLoad() {
	d._pushWal = d.Model.PushWal
	d.Model.PushWal = false
}

func (d *YtDestinationWrapper) ToStorageParams() *YtStorageParams {
	return d.Model.ToStorageParams()
}

func (d *YtDestinationWrapper) Path() string {
	return d.Model.Path
}

func (d *YtDestinationWrapper) Cluster() string {
	return d.Model.Cluster
}

func (d *YtDestinationWrapper) Token() string {
	return d.Model.Token
}

func (d *YtDestinationWrapper) PushWal() bool {
	return d.Model.PushWal
}

func (d *YtDestinationWrapper) NeedArchive() bool {
	return d.Model.NeedArchive
}

func (d *YtDestinationWrapper) CellBundle() string {
	return d.Model.CellBundle
}

func (d *YtDestinationWrapper) TTL() int64 {
	return d.Model.TTL
}

func (d *YtDestinationWrapper) OptimizeFor() string {
	return d.Model.OptimizeFor
}

func (d *YtDestinationWrapper) CanAlter() bool {
	return d.Model.CanAlter
}

func (d *YtDestinationWrapper) IsSchemaMigrationDisabled() bool {
	return d.Model.IsSchemaMigrationDisabled
}

func (d *YtDestinationWrapper) TimeShardCount() int {
	return d.Model.TimeShardCount
}

func (d *YtDestinationWrapper) Index() []string {
	return d.Model.Index
}

func (d *YtDestinationWrapper) HashColumn() string {
	return d.Model.HashColumn
}

func (d *YtDestinationWrapper) PrimaryMedium() string {
	return d.Model.PrimaryMedium
}

func (d *YtDestinationWrapper) Pool() string {
	if d.Model.Pool == "" {
		return poolDefault
	}
	return d.Model.Pool
}

func (d *YtDestinationWrapper) Atomicity() yt.Atomicity {
	return d.Model.Atomicity
}

func (d *YtDestinationWrapper) LoseDataOnError() bool {
	return d.Model.LoseDataOnError
}

func (d *YtDestinationWrapper) DiscardBigValues() bool {
	return d.Model.DiscardBigValues
}

func (d *YtDestinationWrapper) TabletCount() int {
	return d.Model.TabletCount
}

func (d *YtDestinationWrapper) Rotation() *dp_model.RotatorConfig {
	return d.Model.Rotation
}

func (d *YtDestinationWrapper) VersionColumn() string {
	return d.Model.VersionColumn
}

func (d *YtDestinationWrapper) AutoFlushPeriod() int {
	return d.Model.AutoFlushPeriod
}

func (d *YtDestinationWrapper) Ordered() bool {
	return d.Model.Ordered
}

func (d *YtDestinationWrapper) Static() bool {
	return d.Model.Static
}

func (d *YtDestinationWrapper) SortedStatic() bool {
	return d.Model.SortedStatic
}

func (d *YtDestinationWrapper) StaticChunkSize() int {
	if d.Model.StaticChunkSize <= 0 {
		return staticDefaultChunkSize
	}
	return d.Model.StaticChunkSize
}

func (d *YtDestinationWrapper) UseStaticTableOnSnapshot() bool {
	return d.Model.GetUseStaticTableOnSnapshot()
}

func (d *YtDestinationWrapper) AltNames() map[string]string {
	return d.Model.AltNames
}

func (d *YtDestinationWrapper) Spec() *YTSpec {
	return &d.Model.Spec
}

func (d *YtDestinationWrapper) TolerateKeyChanges() bool {
	return d.Model.TolerateKeyChanges
}

func (d *YtDestinationWrapper) InitialTabletCount() uint32 {
	return d.Model.InitialTabletCount
}

func (d *YtDestinationWrapper) WriteTimeoutSec() uint32 {
	return d.Model.WriteTimeoutSec
}

func (d *YtDestinationWrapper) ChunkSize() uint32 {
	return d.Model.ChunkSize
}

func (d *YtDestinationWrapper) BufferTriggingSize() uint64 {
	return d.Model.BufferTriggingSize
}

func (d *YtDestinationWrapper) BufferTriggingInterval() time.Duration {
	return d.Model.BufferTriggingInterval
}

func (d *YtDestinationWrapper) Transformer() map[string]string {
	return d.Model.TransformerConfig
}

func (d *YtDestinationWrapper) CleanupMode() dp_model.CleanupType {
	return d.Model.Cleanup
}

func (d *YtDestinationWrapper) CustomAttributes() map[string]any {
	res := make(map[string]any)
	for key, attr := range d.Model.CustomAttributes {
		var data interface{}
		if err := yson.Unmarshal([]byte(attr), &data); err != nil {
			return nil
		}
		res[key] = data
	}
	return res
}

func (d *YtDestinationWrapper) MergeAttributes(tableSettings map[string]any) map[string]any {
	res := make(map[string]any)
	maps.Copy(res, d.CustomAttributes())
	maps.Copy(res, tableSettings)
	return res
}

func (d *YtDestinationWrapper) WithDefaults() {
	if d.Model.OptimizeFor == "" {
		d.Model.OptimizeFor = "scan"
	}
	if d.Model.PrimaryMedium == "" {
		d.Model.PrimaryMedium = "ssd_blobs"
	}
	if d.Model.Cluster == "" && env.In(env.EnvironmentInternal) {
		d.Model.Cluster = "hahn"
	}
	if d.Model.Pool == "" {
		d.Model.Pool = poolDefault
	}
	if d.Model.Cleanup == "" {
		d.Model.Cleanup = dp_model.Drop
	}
	if d.Model.WriteTimeoutSec == 0 {
		d.Model.WriteTimeoutSec = 60
	}
	if d.Model.ChunkSize == 0 {
		d.Model.ChunkSize = dynamicDefaultChunkSize
	}
	if d.Model.StaticChunkSize == 0 {
		d.Model.StaticChunkSize = staticDefaultChunkSize
	}
	if d.Model.BufferTriggingSize == 0 {
		d.Model.BufferTriggingSize = model.BufferTriggingSizeDefault
	}
}

func (d *YtDestinationWrapper) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    0,
		TriggingSize:     d.BufferTriggingSize(),
		TriggingInterval: d.BufferTriggingInterval(),
	}
}

func (YtDestinationWrapper) IsDestination() {
}

func (d *YtDestinationWrapper) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *YtDestinationWrapper) GetTableAltName(table string) string {
	if d.AltNames() == nil {
		return table
	}
	if altName, ok := d.Model.AltNames[table]; ok {
		return altName
	}
	return table
}

func (d *YtDestinationWrapper) Validate() error {
	d.Model.Rotation = d.Model.Rotation.NilWorkaround()
	if err := d.Model.Rotation.Validate(); err != nil {
		return err
	}
	if !d.Static() && d.CellBundle() == "" {
		return xerrors.New("tablet cell bundle should be set for dynamic table")
	}
	if d.Static() && d.Ordered() {
		return xerrors.New("please choose either static or ordered table, not both")
	}
	if d.Rotation() != nil && d.UseStaticTableOnSnapshot() && !d.Static() && !d.Ordered() {
		return xerrors.Errorf("Not implemented," +
			"not working for dynamic tables with rotation when UseStaticTableOnSnapshot=true" +
			": fix with TM-5114")
	}
	return nil
}

func (d *YtDestinationWrapper) GetConnectionData() ConnectionData {
	return d.Model.Connection
}

func (d *YtDestinationWrapper) DisableProxyDiscovery() bool {
	return d.GetConnectionData().DisableProxyDiscovery
}

func (d *YtDestinationWrapper) Proxy() string {
	return d.Cluster()
}

func (d *YtDestinationWrapper) SupportSharding() bool {
	return !(d.Model.Static && d.Rotation() != nil)
}

// this is kusok govna, it here for purpose - backward compatibility and no reuse without backward compatibility
func (d *YtDestinationWrapper) LegacyModel() interface{} {
	return d.Model
}

func NewYtDestinationV1(model YtDestination) YtDestinationModel {
	return &YtDestinationWrapper{
		Model:    &model,
		_pushWal: false,
	}
}
