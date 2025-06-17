//go:build !disable_clickhouse_provider

package model

import (
	_ "embed"
	"strings"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

var (
	//go:embed doc_source_usage.md
	sourceUsage []byte
	//go:embed doc_source_example.yaml
	sourceExample []byte
)

type ClickhouseIOFormat string

const (
	ClickhouseIOFormatCSV         = ClickhouseIOFormat("CSV")
	ClickhouseIOFormatJSONCompact = ClickhouseIOFormat("JSONCompactEachRow")
	DefaultUser                   = "admin"
)

type ClickHouseShard struct {
	Name  string
	Hosts []string
}

var (
	_ model.Source           = (*ChSource)(nil)
	_ model.Describable      = (*ChSource)(nil)
	_ model.WithConnectionID = (*ChSource)(nil)
)

type ChSource struct {
	MdbClusterID     string `json:"ClusterID"`
	ChClusterName    string // CH cluster from which data will be transfered. Other clusters would be ignored.
	ShardsList       []ClickHouseShard
	HTTPPort         int
	NativePort       int
	User             string
	Password         model.SecretString
	SSLEnabled       bool
	PemFileContent   string
	Database         string
	SubNetworkID     string
	SecurityGroupIDs []string
	IncludeTables    []string
	ExcludeTables    []string
	IsHomo           bool
	BufferSize       uint64
	IOHomoFormat     ClickhouseIOFormat // one of - https://clickhouse.com/docs/en/interfaces/formats
	RootCACertPaths  []string
	ConnectionID     string
}

func (s *ChSource) Describe() model.Doc {
	return model.Doc{
		Usage:   string(sourceUsage),
		Example: string(sourceExample),
	}
}

func (s *ChSource) MDBClusterID() string {
	return s.MdbClusterID
}

func (s *ChSource) WithDefaults() {
	if s.NativePort == 0 {
		s.NativePort = 9440
	}
	if s.HTTPPort == 0 {
		s.HTTPPort = 8443
	}
	if s.BufferSize == 0 {
		s.BufferSize = 50 * 1024 * 1024
	}
}

func (*ChSource) IsSource()                      {}
func (*ChSource) IsIncremental()                 {}
func (*ChSource) SupportsStartCursorValue() bool { return true }

func (s *ChSource) GetProviderType() abstract.ProviderType {
	return "ch"
}

func (s *ChSource) ClusterID() string {
	return s.MdbClusterID
}

func (s *ChSource) Validate() error {
	return nil
}

func (s *ChSource) fulfilledIncludesImpl(tID abstract.TableID, firstIncludeOnly bool) (result []string) {
	tIDVariants := []string{
		tID.Fqtn(),
		strings.Join([]string{tID.Namespace, ".", tID.Name}, ""),
		strings.Join([]string{tID.Namespace, ".", "\"", tID.Name, "\""}, ""),
		strings.Join([]string{tID.Namespace, ".", "*"}, ""),
	}
	tIDNameVariant := strings.Join([]string{"\"", tID.Name, "\""}, tID.Name)
	if s.Database == "*" {
		return []string{tID.Fqtn()}
	}
	for _, table := range s.ExcludeTables {
		for _, variant := range tIDVariants {
			if table == variant {
				return result
			}
		}
		if tID.Namespace == s.Database && (table == tID.Name || table == tIDNameVariant) {
			return result
		}
	}
	if len(s.IncludeTables) == 0 {
		if s.Database == "" {
			return []string{""}
		}
		if s.Database == tID.Namespace {
			return []string{""}
		}
		return result
	}
	for _, table := range s.IncludeTables {
		if tID.Namespace == s.Database && (table == tID.Name || table == tIDNameVariant) {
			result = append(result, table)
			if firstIncludeOnly {
				return result
			}
			continue
		}
		for _, variant := range tIDVariants {
			if table == variant {
				result = append(result, table)
				if firstIncludeOnly {
					return result
				}
				break
			}
		}
	}
	return result
}

func (s *ChSource) Include(tID abstract.TableID) bool {
	return len(s.fulfilledIncludesImpl(tID, true)) > 0
}

func (s *ChSource) FulfilledIncludes(tID abstract.TableID) (result []string) {
	return s.fulfilledIncludesImpl(tID, false)
}

func (s *ChSource) AllIncludes() []string {
	return s.IncludeTables
}

func (s *ChSource) IsAbstract2(dst model.Destination) bool {
	if _, ok := dst.(*ChDestination); ok {
		return true
	}
	return false
}

// SinkParams

type ChSourceWrapper struct {
	Model            *ChSource
	host             *clickhouse.Host // host is here, bcs it needed only in SinkServer/SinkTable
	connectionParams connectionParams
}

func (s ChSourceWrapper) GetIsSchemaMigrationDisabled() bool {
	return false
}

func (s ChSourceWrapper) InsertSettings() InsertParams {
	return InsertParams{MaterializedViewsIgnoreErrors: false}
}

func (s ChSourceWrapper) RootCertPaths() []string {
	return s.Model.RootCACertPaths
}

func (s ChSourceWrapper) ShardByRoundRobin() bool {
	return false
}

func (s ChSourceWrapper) Cleanup() model.CleanupType {
	return model.DisabledCleanup
}

func (s ChSourceWrapper) MdbClusterID() string {
	return s.Model.MdbClusterID
}

func (s ChSourceWrapper) ChClusterName() string {
	return s.Model.ChClusterName
}

func (s ChSourceWrapper) User() string {
	return s.connectionParams.User
}

func (s ChSourceWrapper) Password() string {
	return string(s.Model.Password)
}

func (s ChSourceWrapper) ResolvePassword() (string, error) {
	password, err := ResolvePassword(s.MdbClusterID(), s.User(), string(s.Model.Password))
	return password, err
}

func (s ChSourceWrapper) Database() string {
	return s.Model.Database
}

func (s ChSourceWrapper) Partition() string {
	return ""
}

func (s ChSourceWrapper) Host() *clickhouse.Host {
	return s.host
}

func (s ChSourceWrapper) SSLEnabled() bool {
	return s.Model.SSLEnabled || s.MdbClusterID() != ""
}

func (s ChSourceWrapper) TTL() string {
	return ""
}

func (s ChSourceWrapper) IsUpdateable() bool {
	return false
}

func (s ChSourceWrapper) UpsertAbsentToastedRows() bool {
	return false
}

func (s ChSourceWrapper) InferSchema() bool {
	return false
}

func (s ChSourceWrapper) MigrationOptions() ChSinkMigrationOptions {
	return ChSinkMigrationOptions{false}
}

func (s ChSourceWrapper) UploadAsJSON() bool {
	return false
}

func (s ChSourceWrapper) AnyAsString() bool {
	return false
}

func (s ChSourceWrapper) SystemColumnsFirst() bool {
	return false
}

func (s ChSourceWrapper) AltHosts() []*clickhouse.Host {
	return s.connectionParams.Hosts
}

func (s ChSourceWrapper) UseSchemaInTableName() bool {
	return false
}

func (s ChSourceWrapper) ShardCol() string {
	return ""
}

func (s ChSourceWrapper) Interval() time.Duration {
	return time.Second
}

func (s ChSourceWrapper) BufferTriggingSize() uint64 {
	return 0
}

func (s ChSourceWrapper) Tables() map[string]string {
	return map[string]string{}
}

func (s ChSourceWrapper) ShardByTransferID() bool {
	return false
}

func (s ChSourceWrapper) Rotation() *model.RotatorConfig {
	return nil
}

func (s ChSourceWrapper) Shards() map[string][]*clickhouse.Host {
	return s.connectionParams.Shards
}

func (s ChSourceWrapper) ColumnToShardName() map[string]string {
	return map[string]string{}
}

func (s ChSourceWrapper) PemFileContent() string {
	return s.Model.PemFileContent
}

func (s ChSourceWrapper) GetConnectionID() string {
	return s.Model.GetConnectionID()
}

func (s ChSourceWrapper) MakeChildServerParams(host *clickhouse.Host) ChSinkServerParams {
	newChSource := *s.Model
	newChSourceWrapper := ChSourceWrapper{
		Model:            &newChSource,
		host:             host,
		connectionParams: s.connectionParams,
	}
	return newChSourceWrapper
}

func (s ChSourceWrapper) MakeChildShardParams(altHosts []*clickhouse.Host) ChSinkShardParams {
	newChSource := *s.Model
	newChSourceWrapper := ChSourceWrapper{
		Model:            &newChSource,
		host:             s.host,
		connectionParams: s.connectionParams,
	}

	newChSourceWrapper.connectionParams.Hosts = altHosts

	return newChSourceWrapper
}

func (s ChSourceWrapper) SetShards(shards map[string][]*clickhouse.Host) {
	s.connectionParams.Shards = make(map[string][]*clickhouse.Host)
	s.connectionParams.SetShards(shards)
}

func (s *ChSource) GetConnectionID() string {
	return s.ConnectionID
}
