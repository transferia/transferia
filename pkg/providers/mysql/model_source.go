//go:build !disable_mysql_provider

package mysql

import (
	"hash/fnv"
	"regexp"
	"strings"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

type MysqlFlavorType string

const (
	MysqlFlavorTypeMysql            = "mysql"
	MysqlFlavorTypeMariaDB          = "mariadb"
	DefaultReplicationFlushInterval = time.Second
)

type MysqlTrackerStorage string

type MysqlSource struct {
	Host              string
	User              string
	Password          model.SecretString
	ClusterID         string
	ServerID          uint32
	IncludeTableRegex []string
	ExcludeTableRegex []string
	IsPublic          bool
	Database          string
	TLSFile           string
	SubNetworkID      string
	SecurityGroupIDs  []string
	Port              int
	Timezone          string
	BufferLimit       uint32
	UseFakePrimaryKey bool
	IsHomo            bool `json:"-"`
	PreSteps          *MysqlDumpSteps
	PostSteps         *MysqlDumpSteps
	TrackerDatabase   string

	ConsistentSnapshot          bool
	SnapshotDegreeOfParallelism int
	AllowDecimalAsFloat         bool

	NoTracking  bool // deprecated: use Tracker
	YtTracking  bool // deprecated: use Tracker
	YdbTracking bool // deprecated: use Tracker

	Tracker      MysqlTrackerStorage // deprecated: we only have one tracker now
	PlzNoHomo    bool                // forcefully disable homo features, mostly for tests
	RootCAFiles  []string
	ConnectionID string

	ReplicationFlushInterval time.Duration
}

var (
	_ model.Source           = (*MysqlSource)(nil)
	_ model.WithConnectionID = (*MysqlSource)(nil)
)

type MysqlDumpSteps struct {
	View    bool
	Routine bool
	Trigger bool
	Tables  bool
}

func DefaultMysqlDumpPreSteps() *MysqlDumpSteps {
	return &MysqlDumpSteps{
		Tables:  true,
		View:    false,
		Routine: false,
		Trigger: false,
	}
}

func DefaultMysqlDumpPostSteps() *MysqlDumpSteps {
	return &MysqlDumpSteps{
		Tables:  false,
		View:    false,
		Routine: false,
		Trigger: false,
	}
}

func (s *MysqlSource) InitServerID(transferID string) {
	if s.ServerID != 0 {
		return
	}
	hash := fnv.New32()
	_, _ = hash.Write([]byte(transferID))
	s.ServerID = hash.Sum32()
}

func (s *MysqlSource) fulfilledIncludesImpl(tID abstract.TableID, firstIncludeOnly bool) (result []string) {
	tIDWithSchema := strings.Join([]string{tID.Namespace, ".", tID.Name}, "")

	if s.Database != "" && s.Database != tID.Namespace {
		return result
	}
	for _, excludeRE := range s.ExcludeTableRegex {
		if matches, _ := regexp.MatchString(excludeRE, tID.Name); matches {
			return result
		}
		if matches, _ := regexp.MatchString(excludeRE, tIDWithSchema); matches {
			return result
		}
	}
	if len(s.IncludeTableRegex) == 0 {
		return []string{""}
	}
	for _, includeRE := range s.IncludeTableRegex {
		if matches, _ := regexp.MatchString(includeRE, tID.Name); matches {
			result = append(result, includeRE)
			if firstIncludeOnly {
				return result
			}
		} else if matches, _ := regexp.MatchString(includeRE, tIDWithSchema); matches {
			result = append(result, includeRE)
			if firstIncludeOnly {
				return result
			}
		}
	}
	return result
}

func (s *MysqlSource) Include(tID abstract.TableID) bool {
	return len(s.fulfilledIncludesImpl(tID, true)) > 0
}

func (s *MysqlSource) FulfilledIncludes(tID abstract.TableID) (result []string) {
	return s.fulfilledIncludesImpl(tID, false)
}

func (s *MysqlSource) AllIncludes() []string {
	return s.IncludeTableRegex
}

func (s *MysqlSource) MDBClusterID() string {
	return s.ClusterID
}

func (s *MysqlSource) GetConnectionID() string {
	return s.ConnectionID
}

func (s *MysqlSource) WithDefaults() {
	if s.Port == 0 {
		s.Port = 3306
	}
	if s.BufferLimit == 0 {
		s.BufferLimit = 4 * 1024 * 1024
	}
	if s.PreSteps == nil {
		s.PreSteps = DefaultMysqlDumpPreSteps()
	}
	if s.PostSteps == nil {
		s.PostSteps = DefaultMysqlDumpPostSteps()
	}
	if s.Timezone == "" {
		s.Timezone = "Local"
	}
	if s.SnapshotDegreeOfParallelism <= 0 {
		s.SnapshotDegreeOfParallelism = 4
	}
	if s.ReplicationFlushInterval == 0 {
		s.ReplicationFlushInterval = DefaultReplicationFlushInterval
	}
}

func (s *MysqlSource) HasTLS() bool {
	return s.ClusterID != "" || s.TLSFile != ""
}

func (MysqlSource) IsSource()       {}
func (MysqlSource) IsStrictSource() {}

func (s *MysqlSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *MysqlSource) Validate() error {
	return nil
}

func (s *MysqlSource) ToStorageParams() *MysqlStorageParams {
	return &MysqlStorageParams{
		ClusterID:   s.ClusterID,
		Host:        s.Host,
		Port:        s.Port,
		User:        s.User,
		Password:    string(s.Password),
		Database:    s.Database,
		TLS:         s.HasTLS(),
		CertPEMFile: s.TLSFile,

		UseFakePrimaryKey:   s.UseFakePrimaryKey,
		DegreeOfParallelism: s.SnapshotDegreeOfParallelism,
		Timezone:            s.Timezone,
		ConsistentSnapshot:  s.ConsistentSnapshot,
		RootCAFiles:         s.RootCAFiles,

		TableFilter:  s,
		PreSteps:     s.PreSteps,
		ConnectionID: s.ConnectionID,
	}
}
