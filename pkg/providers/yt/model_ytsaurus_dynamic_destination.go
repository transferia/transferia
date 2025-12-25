package yt

import (
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/maps"
)

type YTSaurusDynamicDestination struct {
	TablePath                      string `log:"true"`
	TableTTL                       int64  `log:"true"` // it's in milliseconds
	TableOptimizeFor               string `log:"true"`
	IsTableSchemaMigrationDisabled bool   `log:"true"`
	TablePrimaryMedium             string `log:"true"`
	UserPool                       string `log:"true"` // pool for running merge and sort operations for static tables
	AtomicityFull                  bool   `log:"true"` // Atomicity for the dynamic tables being created in YT. See https://yt.yandex-team.ru/docs/description/dynamic_tables/sorted_dynamic_tables#atomarnost
	DoDiscardBigValues             bool   `log:"true"`

	DoUseStaticTableOnSnapshot bool                 `log:"true"` // optional.Optional[bool] breaks compatibility
	Cleanup                    dp_model.CleanupType `log:"true"`

	Connection            ConnectionData    `log:"true"`
	TableCustomAttributes map[string]string `log:"true"`
}

var (
	_ dp_model.Destination = (*YTSaurusDynamicDestination)(nil)
)

func (d *YTSaurusDynamicDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

// TODO: Remove in march
func (d *YTSaurusDynamicDestination) DisableDatetimeHack() bool {
	return true
}

func (d *YTSaurusDynamicDestination) CompressionCodec() yt.ClientCompressionCodec {
	return 0
}

func (d *YTSaurusDynamicDestination) ServiceAccountIDs() []string {
	if d.Connection.ServiceAccountID == "" {
		return nil
	}
	return []string{d.Connection.ServiceAccountID}
}

func (d *YTSaurusDynamicDestination) MDBClusterID() string {
	return d.Connection.ClusterID
}

func (d *YTSaurusDynamicDestination) PreSnapshotHacks() {}

func (d *YTSaurusDynamicDestination) PostSnapshotHacks() {}

func (d *YTSaurusDynamicDestination) EnsureTmpPolicySupported() error {
	return xerrors.New("tmp policy is not supported")
}

func (d *YTSaurusDynamicDestination) EnsureCustomTmpPolicySupported() error {
	return xerrors.New("tmp policy is not supported")
}

func (d *YTSaurusDynamicDestination) ToStorageParams() *YtStorageParams {
	return &YtStorageParams{
		Token:                 d.Connection.ServiceAccountID,
		Cluster:               d.Connection.ClusterID,
		Path:                  d.TablePath,
		Spec:                  nil,
		DisableProxyDiscovery: true,
		ConnParams:            d,
	}
}

func (d *YTSaurusDynamicDestination) Path() string {
	return d.TablePath
}

func (d *YTSaurusDynamicDestination) Cluster() string {
	return ""
}

func (d *YTSaurusDynamicDestination) Token() string {
	return ""
}

func (d *YTSaurusDynamicDestination) PushWal() bool {
	return false
}

func (d *YTSaurusDynamicDestination) NeedArchive() bool {
	return false
}

func (d *YTSaurusDynamicDestination) CellBundle() string {
	return "default"
}

func (d *YTSaurusDynamicDestination) TTL() int64 {
	return d.TableTTL
}

func (d *YTSaurusDynamicDestination) OptimizeFor() string {
	return d.TableOptimizeFor
}

func (d *YTSaurusDynamicDestination) IsSchemaMigrationDisabled() bool {
	return d.IsTableSchemaMigrationDisabled
}

func (d *YTSaurusDynamicDestination) TimeShardCount() int {
	return 0
}

func (d *YTSaurusDynamicDestination) Index() []string {
	return []string{}
}

func (d *YTSaurusDynamicDestination) HashColumn() string {
	return ""
}

func (d *YTSaurusDynamicDestination) PrimaryMedium() string {
	return d.TablePrimaryMedium
}

func (d *YTSaurusDynamicDestination) Pool() string {
	if d.UserPool == "" {
		return defaultYTSaurusPool
	}
	return d.UserPool
}

func (d *YTSaurusDynamicDestination) Atomicity() yt.Atomicity { // dynamic tables
	if d.AtomicityFull {
		return yt.AtomicityFull
	}
	return yt.AtomicityNone
}

func (d *YTSaurusDynamicDestination) DiscardBigValues() bool {
	return d.DoDiscardBigValues
}

func (d *YTSaurusDynamicDestination) Rotation() *dp_model.RotatorConfig { // not supported
	return nil
}

func (d *YTSaurusDynamicDestination) VersionColumn() string { // versioned tables
	return ""
}

func (d *YTSaurusDynamicDestination) Ordered() bool { // ordered tables
	return false
}

func (d *YTSaurusDynamicDestination) Static() bool {
	return false
}

func (d *YTSaurusDynamicDestination) SortedStatic() bool {
	return d.UseStaticTableOnSnapshot()
}

func (d *YTSaurusDynamicDestination) StaticChunkSize() int {
	return staticDefaultChunkSize
}

func (d *YTSaurusDynamicDestination) UseStaticTableOnSnapshot() bool { // dynamic tables
	return d.DoUseStaticTableOnSnapshot
}

func (d *YTSaurusDynamicDestination) AltNames() map[string]string { // not supported dont see the point
	return nil
}

func (d *YTSaurusDynamicDestination) Spec() *YTSpec { // Do we need it? Will only be used whe static on snapshot is on
	return new(YTSpec)
}

func (d *YTSaurusDynamicDestination) TolerateKeyChanges() bool { //ordered or versioned
	return false
}

func (d *YTSaurusDynamicDestination) InitialTabletCount() uint32 { //ordered
	return 0
}

func (d *YTSaurusDynamicDestination) WriteTimeoutSec() uint32 {
	return 60
}

func (d *YTSaurusDynamicDestination) ChunkSize() uint32 {
	return dynamicDefaultChunkSize
}

func (d *YTSaurusDynamicDestination) BufferTriggingSize() uint64 {
	return model.BufferTriggingSizeDefault
}

func (d *YTSaurusDynamicDestination) BufferTriggingInterval() time.Duration {
	return 0
}

func (d *YTSaurusDynamicDestination) CleanupMode() dp_model.CleanupType {
	return d.Cleanup
}

func (d *YTSaurusDynamicDestination) CustomAttributes() map[string]any {
	res := make(map[string]any)
	for key, attr := range d.TableCustomAttributes {
		var data interface{}
		if err := yson.Unmarshal([]byte(attr), &data); err != nil {
			return nil
		}
		res[key] = data
	}
	return res
}

func (d *YTSaurusDynamicDestination) MergeAttributes(tableSettings map[string]any) map[string]any {
	res := make(map[string]any)
	maps.Copy(res, d.CustomAttributes())
	maps.Copy(res, tableSettings)
	return res
}

func (d *YTSaurusDynamicDestination) WithDefaults() {
	if d.TableOptimizeFor == "" {
		d.TableOptimizeFor = "scan"
	}
	if d.UserPool == "" {
		d.UserPool = defaultYTSaurusPool
	}
	if d.Cleanup == "" {
		d.Cleanup = dp_model.Drop
	}
	if d.TablePrimaryMedium == "" {
		d.TablePrimaryMedium = "ssd_blobs"
	}
}

func (d *YTSaurusDynamicDestination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    0,
		TriggingSize:     model.BufferTriggingSizeDefault,
		TriggingInterval: 0,
	}
}

func (YTSaurusDynamicDestination) IsDestination() {}

func (d *YTSaurusDynamicDestination) GetProviderType() abstract.ProviderType {
	return ManagedDynamicProviderType
}

func (d *YTSaurusDynamicDestination) GetTableAltName(table string) string {
	return table
}

func (d *YTSaurusDynamicDestination) Validate() error {
	return nil
}

func (d *YTSaurusDynamicDestination) GetConnectionData() ConnectionData {
	return d.Connection
}

func (d *YTSaurusDynamicDestination) DisableProxyDiscovery() bool {
	return true
}

func (d *YTSaurusDynamicDestination) Proxy() string {
	return proxy(d.GetConnectionData().ClusterID)
}

func (d *YTSaurusDynamicDestination) UseTLS() bool {
	return d.GetConnectionData().UseTLS
}

func (d *YTSaurusDynamicDestination) TLSFile() string {
	return d.GetConnectionData().TLSFile
}

func (d *YTSaurusDynamicDestination) ServiceAccountID() string {
	return d.GetConnectionData().ServiceAccountID
}

func (d *YTSaurusDynamicDestination) ProxyRole() string {
	return ""
}

func (d *YTSaurusDynamicDestination) SupportSharding() bool {
	return false
}

// this is kusok govna, it here for purpose - backward compatibility and no reuse without backward compatibility
func (d *YTSaurusDynamicDestination) LegacyModel() interface{} {
	return d
}
