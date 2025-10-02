package yt

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/maps"
)

const (
	defaultYTSaurusPool = "default"
)

func proxy(clusterID string) string {
	return fmt.Sprintf("https://%s.proxy.ytsaurus.yandexcloud.net", clusterID)
}

type YTSaurusStaticDestination struct {
	TablePath             string
	TableOptimizeFor      string
	UserPool              string // pool for running merge and sort operations for static tables
	DoDiscardBigValues    bool
	TableCustomAttributes map[string]string
	Cleanup               dp_model.CleanupType
	Connection            ConnectionData
	IsSortedStatic        bool // true, if we need to sort static tables
}

var (
	_ dp_model.Destination = (*YTSaurusStaticDestination)(nil)
)

// TODO: Remove in march
func (d *YTSaurusStaticDestination) DisableDatetimeHack() bool {
	return true
}

func (d *YTSaurusStaticDestination) CompressionCodec() yt.ClientCompressionCodec {
	return 0
}

func (d *YTSaurusStaticDestination) MDBClusterID() string {
	return d.Connection.ClusterID
}

func (d *YTSaurusStaticDestination) PreSnapshotHacks() {}

func (d *YTSaurusStaticDestination) PostSnapshotHacks() {}

func (d *YTSaurusStaticDestination) EnsureTmpPolicySupported() error {
	return xerrors.New("tmp policy is not supported")
}

func (d *YTSaurusStaticDestination) EnsureCustomTmpPolicySupported() error {
	return xerrors.New("tmp policy is not supported")
}

func (d *YTSaurusStaticDestination) ToStorageParams() *YtStorageParams {
	return &YtStorageParams{
		Token:                 d.Connection.ServiceAccountID,
		Cluster:               d.Connection.ClusterID,
		Path:                  d.TablePath,
		Spec:                  nil,
		DisableProxyDiscovery: true,
		ConnParams:            d,
	}
}

func (d *YTSaurusStaticDestination) Path() string {
	return d.TablePath
}

func (d *YTSaurusStaticDestination) Cluster() string {
	return ""
}

func (d *YTSaurusStaticDestination) Token() string {
	return ""
}

func (d *YTSaurusStaticDestination) PushWal() bool {
	return false
}

func (d *YTSaurusStaticDestination) NeedArchive() bool {
	return false
}

func (d *YTSaurusStaticDestination) CellBundle() string {
	return "default"
}

func (d *YTSaurusStaticDestination) TTL() int64 {
	return 0
}

func (d *YTSaurusStaticDestination) OptimizeFor() string {
	return d.TableOptimizeFor
}

func (d *YTSaurusStaticDestination) IsSchemaMigrationDisabled() bool {
	return false
}

func (d *YTSaurusStaticDestination) TimeShardCount() int {
	return 0
}

func (d *YTSaurusStaticDestination) Index() []string {
	return []string{}
}

func (d *YTSaurusStaticDestination) HashColumn() string {
	return ""
}

func (d *YTSaurusStaticDestination) PrimaryMedium() string {
	return "ssd_blobs"
}

func (d *YTSaurusStaticDestination) Pool() string {
	if d.UserPool == "" {
		return defaultYTSaurusPool
	}
	return d.UserPool
}

func (d *YTSaurusStaticDestination) Atomicity() yt.Atomicity { // dynamic tables
	return yt.AtomicityNone
}

func (d *YTSaurusStaticDestination) DiscardBigValues() bool {
	return d.DoDiscardBigValues
}

func (d *YTSaurusStaticDestination) Rotation() *dp_model.RotatorConfig { // not supported
	return nil
}

func (d *YTSaurusStaticDestination) VersionColumn() string { // versioned tables
	return ""
}

func (d *YTSaurusStaticDestination) Ordered() bool { // ordered tables
	return false
}

func (d *YTSaurusStaticDestination) Static() bool {
	return true
}

func (d *YTSaurusStaticDestination) SortedStatic() bool {
	return d.IsSortedStatic
}

func (d *YTSaurusStaticDestination) StaticChunkSize() int {
	return staticDefaultChunkSize
}

func (d *YTSaurusStaticDestination) UseStaticTableOnSnapshot() bool { // dynamic tables
	return false
}

func (d *YTSaurusStaticDestination) AltNames() map[string]string { // not supported dont see the point
	return nil
}

func (d *YTSaurusStaticDestination) Spec() *YTSpec {
	return new(YTSpec)
}

func (d *YTSaurusStaticDestination) TolerateKeyChanges() bool { //ordered or versioned
	return false
}

func (d *YTSaurusStaticDestination) InitialTabletCount() uint32 { //ordered
	return 0
}

func (d *YTSaurusStaticDestination) WriteTimeoutSec() uint32 {
	return 60
}

func (d *YTSaurusStaticDestination) ChunkSize() uint32 {
	return dynamicDefaultChunkSize
}

func (d *YTSaurusStaticDestination) BufferTriggingSize() uint64 {
	return model.BufferTriggingSizeDefault
}

func (d *YTSaurusStaticDestination) BufferTriggingInterval() time.Duration {
	return 0
}

func (d *YTSaurusStaticDestination) CleanupMode() dp_model.CleanupType {
	return d.Cleanup
}

func (d *YTSaurusStaticDestination) CustomAttributes() map[string]any {
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

func (d *YTSaurusStaticDestination) MergeAttributes(tableSettings map[string]any) map[string]any {
	res := make(map[string]any)
	maps.Copy(res, d.CustomAttributes())
	maps.Copy(res, tableSettings)
	return res
}

func (d *YTSaurusStaticDestination) WithDefaults() {
	if d.TableOptimizeFor == "" {
		d.TableOptimizeFor = "scan"
	}
	if d.UserPool == "" {
		d.UserPool = defaultYTSaurusPool
	}
	if d.Cleanup == "" {
		d.Cleanup = dp_model.Drop
	}
}

func (d *YTSaurusStaticDestination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    0,
		TriggingSize:     model.BufferTriggingSizeDefault,
		TriggingInterval: 0,
	}
}

func (YTSaurusStaticDestination) IsDestination() {}

func (d *YTSaurusStaticDestination) GetProviderType() abstract.ProviderType {
	return ManagedStaticProviderType
}

func (d *YTSaurusStaticDestination) GetTableAltName(table string) string {
	return table
}

func (d *YTSaurusStaticDestination) Validate() error {
	return nil
}

func (d *YTSaurusStaticDestination) GetConnectionData() ConnectionData {
	return d.Connection
}

func (d *YTSaurusStaticDestination) DisableProxyDiscovery() bool {
	return true
}

func (d *YTSaurusStaticDestination) Proxy() string {
	return proxy(d.GetConnectionData().ClusterID)
}

func (d *YTSaurusStaticDestination) UseTLS() bool {
	return d.GetConnectionData().UseTLS
}

func (d *YTSaurusStaticDestination) TLSFile() string {
	return d.GetConnectionData().TLSFile
}

func (d *YTSaurusStaticDestination) ServiceAccountID() string {
	return d.GetConnectionData().ServiceAccountID
}

func (d *YTSaurusStaticDestination) ProxyRole() string {
	return ""
}

func (d *YTSaurusStaticDestination) SupportSharding() bool {
	return false
}

// this is kusok govna, it here for purpose - backward compatibility and no reuse without backward compatibility
func (d *YTSaurusStaticDestination) LegacyModel() interface{} {
	return d
}
