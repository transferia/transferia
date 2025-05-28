package postgres

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/providers/postgres/dblog"
	"github.com/transferia/transferia/pkg/providers/postgres/utils"
	"github.com/transferia/transferia/pkg/storage"
	"github.com/transferia/transferia/pkg/transformer/registry/rename"
	"github.com/transferia/transferia/pkg/util"
)

const pgDesiredTableSize = 1024 * 1024 * 1024

var PGGlobalExclude = []abstract.TableID{
	{Namespace: "public", Name: "repl_mon"},
}

type PgSource struct {
	// oneof
	ClusterID string `json:"Cluster"`
	Host      string // legacy field for back compatibility; for now, we are using only 'Hosts' field
	Hosts     []string

	Database                    string
	User                        string
	Password                    model.SecretString
	Port                        int
	DBTables                    []string
	BatchSize                   uint32 // BatchSize is a limit on the number of rows in the replication (not snapshot) source internal buffer
	SlotID                      string
	SlotByteLagLimit            int64
	TLSFile                     string
	EnableTLS                   bool
	KeeperSchema                string
	SubNetworkID                string
	SecurityGroupIDs            []string
	CollapseInheritTables       bool
	UsePolling                  bool
	ExcludedTables              []string
	IsHomo                      bool
	NoHomo                      bool // force hetero relations instead of homo-haram-stuff
	AutoActivate                bool
	PreSteps                    *PgDumpSteps
	PostSteps                   *PgDumpSteps
	UseFakePrimaryKey           bool
	IgnoreUserTypes             bool
	IgnoreUnknownTables         bool            // see: if table schema unknown - ignore it TM-3104 and TM-2934
	MaxBufferSize               model.BytesSize // Deprecated: is not used anymore
	ExcludeDescendants          bool            // Deprecated: is not used more, use CollapseInheritTables instead
	DesiredTableSize            uint64          // desired table part size for snapshot sharding
	SnapshotDegreeOfParallelism int             // desired table parts count for snapshot sharding
	EmitTimeTypes               bool            // Deprecated: is not used anymore

	DBLogEnabled bool   // force DBLog snapshot instead of common
	ChunkSize    uint64 // number of rows in chunk, this field needed for DBLog snapshot, if it is 0, it will be calculated automatically

	// Whether text or binary serialization format should be used when readeing
	// snapshot from PostgreSQL storage snapshot (see
	// https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-FORMAT-CODES).
	// If not specified (i.e. equal to PgSerializationFormatAuto), defaults to
	// "binary" for homogeneous pg->pg transfer, and to "text" in all other
	// cases. Binary is preferred for pg->pg transfers since we use CopyFrom
	// function from pgx driver and it requires all values to be binary
	// serializable.
	SnapshotSerializationFormat PgSerializationFormat
	ShardingKeyFields           map[string][]string
	PgDumpCommand               []string
	ConnectionID                string
}

var (
	_ model.Source           = (*PgSource)(nil)
	_ model.WithConnectionID = (*PgSource)(nil)
)

func (s *PgSource) MDBClusterID() string {
	return s.ClusterID
}

func (s *PgSource) GetConnectionID() string {
	return s.ConnectionID
}

type PgSerializationFormat string

const (
	PgSerializationFormatAuto   = PgSerializationFormat("")
	PgSerializationFormatText   = PgSerializationFormat("text")
	PgSerializationFormatBinary = PgSerializationFormat("binary")
)

type PgDumpSteps struct {
	Table, PrimaryKey, View, Sequence         bool
	SequenceOwnedBy, Rule, Type               bool
	Constraint, FkConstraint, Index, Function bool
	Collation, Trigger, Policy, Cast          bool
	Default                                   bool
	MaterializedView                          bool
	SequenceSet                               *bool
	TableAttach                               bool
	IndexAttach                               bool
}

func DefaultPgDumpPreSteps() *PgDumpSteps {
	return &PgDumpSteps{
		Table:            true,
		TableAttach:      true,
		PrimaryKey:       true,
		View:             true,
		MaterializedView: true,
		Sequence:         true,
		SequenceSet:      util.TruePtr(),
		SequenceOwnedBy:  true,
		Rule:             true,
		Type:             true,
		Default:          true,
		Function:         true,
		Collation:        true,
		Policy:           true,

		Constraint:   false,
		FkConstraint: false,
		Index:        false,
		IndexAttach:  false,
		Trigger:      false,
		Cast:         false,
	}
}

func DefaultPgDumpPostSteps() *PgDumpSteps {
	steps := &PgDumpSteps{
		Constraint:   true,
		FkConstraint: true,
		Index:        true,
		IndexAttach:  true,
		Trigger:      true,

		Table:            false,
		TableAttach:      false,
		PrimaryKey:       false,
		View:             false,
		MaterializedView: false,
		Sequence:         false,
		SequenceSet:      util.FalsePtr(),
		SequenceOwnedBy:  false,
		Rule:             false,
		Type:             false,
		Default:          false,
		Function:         false,
		Collation:        false,
		Policy:           false,
		Cast:             false,
	}
	return steps
}

func (s *PgSource) FillDependentFields(transfer *model.Transfer) {
	if s.SlotID == "" {
		s.SlotID = transfer.ID
	}
	if _, ok := transfer.Dst.(*PgDestination); ok && !s.NoHomo {
		s.IsHomo = true
	}
}

// AllHosts - function to move from legacy 'Host' into modern 'Hosts'
func (s *PgSource) AllHosts() []string {
	return utils.HandleHostAndHosts(s.Host, s.Hosts)
}

func (s *PgSource) HasTLS() bool {
	return s.TLSFile != "" || s.EnableTLS
}

func (s *PgSource) fulfilledIncludesImpl(tID abstract.TableID, firstIncludeOnly bool) (result []string) {
	// A map could be used here, but for such a small array it is likely inefficient
	tIDVariants := []string{
		tID.Fqtn(),
		strings.Join([]string{tID.Namespace, ".", tID.Name}, ""),
		strings.Join([]string{tID.Namespace, ".", "\"", tID.Name, "\""}, ""),
		strings.Join([]string{tID.Namespace, ".", "*"}, ""),
	}
	tIDNameVariant := strings.Join([]string{"\"", tID.Name, "\""}, "")

	for _, table := range PGGlobalExclude {
		if table == tID {
			return result
		}
	}
	for _, table := range s.ExcludedTables {
		if tID.Namespace == "public" && (table == tID.Name || table == tIDNameVariant) {
			return result
		}
		for _, variant := range tIDVariants {
			if table == variant {
				return result
			}
		}
	}
	if len(s.DBTables) == 0 {
		return []string{""}
	}
	for _, table := range s.DBTables {
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
	if tID.Namespace == s.KeeperSchema {
		switch tID.Name {
		case TableConsumerKeeper, TableLSN, dblog.SignalTableName:
			result = append(result, abstract.PgName(s.KeeperSchema, tID.Name))
		}
	}
	return result
}

func (s *PgSource) Include(tID abstract.TableID) bool {
	return len(s.fulfilledIncludesImpl(tID, true)) > 0
}

func (s *PgSource) FulfilledIncludes(tID abstract.TableID) (result []string) {
	return s.fulfilledIncludesImpl(tID, false)
}

func (s *PgSource) AllIncludes() []string {
	return s.DBTables
}

func (s *PgSource) AuxTables() []string {
	return []string{
		abstract.PgName(s.KeeperSchema, TableConsumerKeeper),
		abstract.PgName(s.KeeperSchema, TableLSN),
		abstract.PgName(s.KeeperSchema, dblog.SignalTableName),
	}
}

func (s *PgSource) WithDefaults() {
	if s.SlotByteLagLimit == 0 {
		s.SlotByteLagLimit = 50 * 1024 * 1024 * 1024
	}

	if s.BatchSize == 0 {
		s.BatchSize = 1024
	}

	if s.Port == 0 {
		s.Port = 6432
	}

	if s.KeeperSchema == "" {
		s.KeeperSchema = "public"
	}

	if s.PreSteps == nil {
		s.PreSteps = DefaultPgDumpPreSteps()
	}
	if s.PostSteps == nil {
		s.PostSteps = DefaultPgDumpPostSteps()
	}
	if s.DesiredTableSize == 0 {
		s.DesiredTableSize = pgDesiredTableSize
	}
	if s.SnapshotDegreeOfParallelism == 0 {
		s.SnapshotDegreeOfParallelism = 4 // old magic number which was hardcoded in shard table func
	}
}

func (s *PgSource) ExcludeWithGlobals() []string {
	excludes := s.ExcludedTables
	for _, table := range PGGlobalExclude {
		excludes = append(excludes, table.Fqtn())
	}
	return excludes
}

func (*PgSource) IsSource()                      {}
func (*PgSource) IsStrictSource()                {}
func (*PgSource) IsIncremental()                 {}
func (*PgSource) SupportsStartCursorValue() bool { return true }

func (s *PgSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (p *PgDumpSteps) List() []string {
	var res []string
	if p == nil {
		return res
	}
	v := reflect.ValueOf(*p)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		var ok bool
		if field.Kind() == reflect.Pointer {
			if field.IsNil() {
				ok = true
			} else {
				ok = field.Elem().Bool()
			}
		} else {
			ok = field.Bool()
		}
		if ok {
			pgDumpItemType := strings.ToUpper(util.Snakify(v.Type().Field(i).Name))
			res = append(res, pgDumpItemType)
		}
	}
	return res
}

func (p *PgDumpSteps) AnyStepIsTrue() bool {
	if p == nil {
		return false
	}
	sequenceSet := false
	if p.SequenceSet != nil {
		sequenceSet = *p.SequenceSet
	}

	return slices.Contains([]bool{
		p.Sequence, p.SequenceOwnedBy, sequenceSet, p.Table, p.PrimaryKey, p.FkConstraint,
		p.Default, p.Constraint, p.Index, p.View, p.MaterializedView, p.Function, p.Trigger,
		p.Type, p.Rule, p.Collation, p.Policy, p.Cast,
	}, true)
}

func (s *PgSource) Validate() error {
	if err := utils.ValidatePGTables(s.DBTables); err != nil {
		return xerrors.Errorf("validate include tables error: %w", err)
	}
	if err := utils.ValidatePGTables(s.ExcludedTables); err != nil {
		return xerrors.Errorf("validate exclude tables error: %w", err)
	}
	return nil
}

func (s *PgSource) ExtraTransformers(ctx context.Context, transfer *model.Transfer, registry metrics.Registry) ([]abstract.Transformer, error) {
	var result []abstract.Transformer
	if s.CollapseInheritTables {
		pgStorageAbstract, err := storage.NewStorage(transfer, coordinator.NewFakeClient(), registry)
		if err != nil {
			return nil, errors.CategorizedErrorf(categories.Source, "unable to resolve PG storage from transfer source: %w", err)
		}
		pgStorage, ok := pgStorageAbstract.(*Storage)
		if !ok {
			return nil, errors.CategorizedErrorf(categories.Source, "storage is not of type '%T', actual type '%T'", pgStorage, pgStorageAbstract)
		}
		inheritedTables, err := pgStorage.GetInheritedTables(ctx)
		if err != nil {
			return nil, errors.CategorizedErrorf(categories.Source, "CollapseInheritTables option is set: cannot init rename for inherited tables: %w", err)
		}
		result = append(result, &rename.RenameTableTransformer{AltNames: inheritedTables})
	}
	return result, nil
}

// SinkParams

type PgSourceWrapper struct {
	Model          *PgSource
	maintainTables bool
}

func (d *PgSourceWrapper) SetMaintainTables(maintainTables bool) {
	d.maintainTables = maintainTables
}

func (d PgSourceWrapper) ClusterID() string {
	return d.Model.ClusterID
}

func (d PgSourceWrapper) AllHosts() []string {
	return d.Model.AllHosts()
}

func (d PgSourceWrapper) Port() int {
	return d.Model.Port
}

func (d PgSourceWrapper) Database() string {
	return d.Model.Database
}

func (d PgSourceWrapper) User() string {
	return d.Model.User
}

func (d PgSourceWrapper) Password() string {
	return string(d.Model.Password)
}

func (d PgSourceWrapper) HasTLS() bool {
	return d.Model.HasTLS()
}

func (d PgSourceWrapper) TLSFile() string {
	return d.Model.TLSFile
}

func (d PgSourceWrapper) MaintainTables() bool {
	return d.maintainTables
}

func (d PgSourceWrapper) ConnectionID() string {
	return d.Model.ConnectionID
}

func (d PgSourceWrapper) PerTransactionPush() bool {
	return false
}

func (d PgSourceWrapper) LoozeMode() bool {
	return false
}

func (d PgSourceWrapper) CleanupMode() model.CleanupType {
	return model.Drop
}

func (d PgSourceWrapper) Tables() map[string]string {
	return map[string]string{}
}

func (d PgSourceWrapper) CopyUpload() bool {
	return false
}

func (d PgSourceWrapper) IgnoreUniqueConstraint() bool {
	return false
}

func (d PgSourceWrapper) DisableSQLFallback() bool {
	return false
}

func (d PgSourceWrapper) QueryTimeout() time.Duration {
	return PGDefaultQueryTimeout
}

func (d PgSourceWrapper) GetIsSchemaMigrationDisabled() bool {
	return false
}

func (s *PgSource) ToSinkParams() PgSourceWrapper {
	copyPgWrapper := *s
	return PgSourceWrapper{
		Model:          &copyPgWrapper,
		maintainTables: false,
	}
}

func (s *PgSource) isPreferReplica(transfer *model.Transfer) bool {
	// PreferReplica auto-derives into 'true', if ALL next properties fulfilled:
	// - It can be used only on 'managed' installation - bcs we are searching replicas via mdb api
	// - It can be used only on heterogeneous transfers - bcs "for homo there are some technical restrictions" (https://github.com/transferia/transferia/review/4059241/details#comment-5973004)
	//     There are some issues with reading sequence values from replica
	// - It can be used only on SNAPSHOT_ONLY transfer - bcs we can't take consistent slot on master & snapshot on replica
	// - It can be used only when DBLog is disabled - bcs DBLog requires master connection to insert/update records in signal table
	//
	// When 'PreferReplica' is true - reading happens from synchronous replica
	return !s.IsHomo && transfer != nil && (transfer.SnapshotOnly() || !transfer.IncrementOnly()) && !s.DBLogEnabled
}

func (s *PgSource) ToStorageParams(transfer *model.Transfer) *PgStorageParams {
	var useBinarySerialization bool
	if s.SnapshotSerializationFormat == PgSerializationFormatAuto {
		useBinarySerialization = s.IsHomo
	} else {
		useBinarySerialization = s.SnapshotSerializationFormat == PgSerializationFormatBinary
	}

	return &PgStorageParams{
		AllHosts:                    s.AllHosts(),
		Port:                        s.Port,
		User:                        s.User,
		Password:                    string(s.Password),
		Database:                    s.Database,
		ClusterID:                   s.ClusterID,
		TLSFile:                     s.TLSFile,
		EnableTLS:                   s.EnableTLS,
		UseFakePrimaryKey:           s.UseFakePrimaryKey,
		DBFilter:                    nil,
		IgnoreUserTypes:             s.IgnoreUserTypes,
		PreferReplica:               s.isPreferReplica(transfer),
		ExcludeDescendants:          s.ExcludeDescendants,
		DesiredTableSize:            s.DesiredTableSize,
		SnapshotDegreeOfParallelism: s.SnapshotDegreeOfParallelism,
		ConnString:                  "",
		TableFilter:                 s,
		TryHostCACertificates:       false,
		UseBinarySerialization:      useBinarySerialization,
		SlotID:                      s.SlotID,
		ShardingKeyFields:           s.ShardingKeyFields,
		ConnectionID:                s.ConnectionID,
	}
}
