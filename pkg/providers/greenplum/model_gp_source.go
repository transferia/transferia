//go:build !disable_greenplum_provider

package greenplum

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/utils"
)

type GpSource struct {
	Connection       GpConnection
	IncludeTables    []string
	ExcludeTables    []string
	AdvancedProps    GpSourceAdvancedProps
	SubnetID         string
	SecurityGroupIDs []string
	GpfdistParams    gpfdistbin.GpfdistParams
}

var _ model.Source = (*GpSource)(nil)

func (s *GpSource) MDBClusterID() string {
	if s.Connection.MDBCluster != nil {
		return s.Connection.MDBCluster.ClusterID
	}
	return ""
}

func (s *GpSource) IsSource()       {}
func (s *GpSource) IsStrictSource() {}

type GpSourceAdvancedProps struct {
	// EnforceConsistency enables *enforcement* of consistent snapshot. When it is not set, the user is responsible for snapshot consistency
	EnforceConsistency bool

	ServiceSchema string

	// AllowCoordinatorTxFailure disables coordinator TX monitoring (liveness monitor) and enables the transfer to finish snapshot successfully even if the coordinator TX fails
	AllowCoordinatorTxFailure    bool
	LivenessMonitorCheckInterval time.Duration
}

func (p *GpSourceAdvancedProps) Validate() error {
	return nil
}

func (p *GpSourceAdvancedProps) WithDefaults() {
	if len(p.ServiceSchema) == 0 {
		p.ServiceSchema = "public"
	}
	if p.LivenessMonitorCheckInterval == 0 {
		p.LivenessMonitorCheckInterval = 30 * time.Second
	}
}

type GpConnection struct {
	MDBCluster *MDBClusterCreds
	OnPremises *GpCluster
	Database   string
	User       string
	AuthProps  PgAuthProps
}

type PgAuthProps struct {
	Password      model.SecretString
	CACertificate string
}

type MDBClusterCreds struct {
	ClusterID string
}

func (s *GpHP) Validate() error {
	if len(s.Host) == 0 {
		return xerrors.New("missing host")
	}
	if s.Port == 0 {
		return xerrors.New("missing port")
	}
	return nil
}

func (s *GpHAP) Validate() error {
	if s.Primary == nil {
		return xerrors.New("missing primary segment")
	}
	if err := s.Primary.Validate(); err != nil {
		return xerrors.Errorf("failed to validate primary segment: %w", err)
	}
	if s.Mirror != nil {
		if err := s.Mirror.Validate(); err != nil {
			return xerrors.Errorf("failed to validate mirror segment: %w", err)
		}
	}
	return nil
}

func (c *GpConnection) Validate() error {
	if c.MDBCluster == nil && c.OnPremises == nil {
		return xerrors.New("missing either MDB cluster ID or on-premises connection properties")
	}
	if c.OnPremises != nil {
		if c.OnPremises.Coordinator == nil {
			return xerrors.New("missing on-premises coordinator")
		}
		if err := c.OnPremises.Coordinator.Validate(); err != nil {
			return xerrors.Errorf("failed to validate on-premises coordinator: %w", err)
		}
		for i, pair := range c.OnPremises.Segments {
			if pair == nil {
				return xerrors.Errorf("unspecified on-premises segment №%d", i)
			}
			if err := pair.Validate(); err != nil {
				return xerrors.Errorf("failed to validate on-premises segment №%d: %w", i, err)
			}
		}
	}
	if len(c.User) == 0 {
		return xerrors.New("missing user for database access")
	}
	if len(c.Database) == 0 {
		return xerrors.New("missing database name")
	}
	return nil
}

func (c *GpConnection) WithDefaults() {
	if c.MDBCluster == nil && c.OnPremises == nil {
		c.MDBCluster = new(MDBClusterCreds)
	}
	if len(c.User) == 0 {
		c.User = "gpadmin"
	}
	if len(c.Database) == 0 {
		c.Database = "postgres"
	}
}

type GpCluster struct {
	Coordinator *GpHAP
	Segments    []*GpHAP
}

func (s *GpCluster) SegByID(id int) *GpHAP {
	if id < -1 || id >= len(s.Segments) {
		logger.Log.Errorf("SegByID is called with a faulty value %d", id)
		id = -1
	}
	if id == -1 {
		return s.Coordinator
	}
	return s.Segments[id]
}

// GpHAP stands for "Greenplum Highly Available host Pair"
type GpHAP struct {
	Primary *GpHP
	Mirror  *GpHP
}

func (s *GpHAP) AnyAvailable() (*GpHP, error) {
	if s.Primary != nil && s.Primary.Valid() {
		return s.Primary, nil
	}
	if s.Mirror != nil && s.Mirror.Valid() {
		return s.Mirror, nil
	}
	return nil, xerrors.New("Neither primary nor mirror are available")
}

func (s *GpHAP) String() string {
	if s.Mirror == nil || !s.Mirror.Valid() {
		return strings.Join([]string{s.Primary.String(), "no mirror"}, " / ")
	}
	if s.Primary == nil || !s.Primary.Valid() {
		return strings.Join([]string{"no primary", s.Mirror.String()}, " / ")
	}
	return strings.Join([]string{s.Primary.String(), s.Mirror.String()}, " / ")
}

type greenplumHAPair interface {
	GetPrimaryHost() string
	GetPrimaryPort() int64

	GetMirrorHost() string
	GetMirrorPort() int64
}

func GpHAPFromGreenplumUIHAPair(hap greenplumHAPair) *GpHAP {
	var mirror *GpHP
	if hap.GetMirrorHost() != "" && hap.GetMirrorPort() != 0 {
		mirror = &GpHP{
			hap.GetMirrorHost(),
			int(hap.GetMirrorPort()),
		}
	}

	pair := &GpHAP{
		Primary: &GpHP{
			hap.GetPrimaryHost(),
			int(hap.GetPrimaryPort()),
		},
		Mirror: mirror,
	}
	return pair
}

// GpHP stands for "Greenplum Host/Port"
type GpHP struct {
	Host string
	Port int
}

func NewGpHP(host string, port int) *GpHP {
	return &GpHP{
		Host: host,
		Port: port,
	}
}

// NewGpHpWithMDBReplacement replaces domain names for Cloud Preprod & Prod and returns a new host-port pair
func NewGpHpWithMDBReplacement(host string, port int) *GpHP {
	if mdbPreprodDomainRe.MatchString(host) {
		host = mdbPreprodDomainRe.ReplaceAllLiteralString(host, mdbServiceDomainExternalCloud)
	} else if mdbProdDomainRe.MatchString(host) {
		host = mdbProdDomainRe.ReplaceAllLiteralString(host, mdbServiceDomainExternalCloud)
	} else if mdbInternalProdDomainRe.MatchString(host) {
		host = mdbInternalProdDomainRe.ReplaceAllLiteralString(host, mdbServiceDomainInternalCloud)
	}
	return NewGpHP(host, port)
}

var (
	mdbPreprodDomainRe      = regexp.MustCompile(`\.mdb\.cloud-preprod\.yandex\.net$`)
	mdbProdDomainRe         = regexp.MustCompile(`\.mdb\.yandexcloud\.net$`)
	mdbInternalProdDomainRe = regexp.MustCompile(`\.db\.yandex\.net$`)
)

const (
	mdbServiceDomainExternalCloud = ".db.yandex.net"
	mdbServiceDomainInternalCloud = ".mdb.yandex.net"
)

func (s *GpHP) String() string {
	if !s.Valid() {
		return "<missing>"
	}
	return strings.Join([]string{s.Host, strconv.Itoa(s.Port)}, ":")
}

func (s *GpHP) Valid() bool {
	return len(s.Host) > 0
}

func (s *GpSource) WithDefaults() {
	s.Connection.WithDefaults()
	s.AdvancedProps.WithDefaults()
	s.GpfdistParams.WithDefaults()
}

func (s *GpSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *GpSource) Validate() error {
	if err := s.Connection.Validate(); err != nil {
		return xerrors.Errorf("invalid connection parameters: %w", err)
	}
	if err := s.AdvancedProps.Validate(); err != nil {
		return xerrors.Errorf("invalid advanced connection parameters: %w", err)
	}
	if err := utils.ValidatePGTables(s.IncludeTables); err != nil {
		return xerrors.Errorf("validate include tables error: %w", err)
	}
	if err := utils.ValidatePGTables(s.ExcludeTables); err != nil {
		return xerrors.Errorf("validate exclude tables error: %w", err)
	}
	return nil
}

func (s *GpSource) fulfilledIncludesImpl(tID abstract.TableID, firstIncludeOnly bool) (result []string) {
	// A map could be used here, but for such a small array it is likely inefficient
	tIDVariants := []string{
		tID.Fqtn(),
		strings.Join([]string{tID.Namespace, ".", tID.Name}, ""),
		strings.Join([]string{tID.Namespace, ".", "\"", tID.Name, "\""}, ""),
		strings.Join([]string{tID.Namespace, ".", "*"}, ""),
	}
	tIDNameVariant := strings.Join([]string{"\"", tID.Name, "\""}, "")

	for _, table := range postgres.PGGlobalExclude {
		if table == tID {
			return result
		}
	}
	for _, table := range s.ExcludeTables {
		if tID.Namespace == "public" && (table == tID.Name || table == tIDNameVariant) {
			return result
		}
		for _, variant := range tIDVariants {
			if table == variant {
				return result
			}
		}
	}
	if len(s.IncludeTables) == 0 {
		return []string{""}
	}
	for _, table := range s.IncludeTables {
		if tID.Namespace == "public" && (table == tID.Name || table == tIDNameVariant) {
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

func (s *GpSource) Include(tID abstract.TableID) bool {
	return len(s.fulfilledIncludesImpl(tID, true)) > 0
}

func (s *GpSource) FulfilledIncludes(tID abstract.TableID) (result []string) {
	return s.fulfilledIncludesImpl(tID, false)
}

func (s *GpSource) AllIncludes() []string {
	return s.IncludeTables
}
