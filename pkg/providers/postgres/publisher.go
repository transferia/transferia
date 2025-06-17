//go:build !disable_postgres_provider

package postgres

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres/dblog"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type Plugin string

type argument struct {
	name, value string
}

var commonWal2jsonArguments = []argument{
	{name: "include-timestamp", value: "1"},
	{name: "include-types", value: "1"},
	{name: "include-xids", value: "1"},
	{name: "include-type-oids", value: "1"},
	{name: "write-in-chunks", value: "1"},
}

type wal2jsonArguments []argument

func newWal2jsonArguments(config *PgSource, objects *model.DataObjects, dbLogSnapshot bool) (wal2jsonArguments, error) {
	var result []argument

	result = append(result, commonWal2jsonArguments...)

	addList, err := addTablesList(config, objects, dbLogSnapshot)
	if err != nil {
		return nil, xerrors.Errorf("failed to compose a list of included tables: %w", err)
	}
	if len(addList) > 0 {
		result = append(result, argument{name: "add-tables", value: wal2jsonTableList(addList)})
	}

	filterList, err := filterTablesList(config)
	if err != nil {
		return nil, xerrors.Errorf("failed to compose a list of included tables: %w", err)
	}
	if len(filterList) > 0 {
		result = append(result, argument{name: "filter-tables", value: wal2jsonTableList(filterList)})
	}

	return result, nil
}

func (a wal2jsonArguments) toReplicationFormat() []string {
	var result []string
	for _, arg := range a {
		result = append(result, fmt.Sprintf(`"%s" '%s'`, arg.name, arg.value))
	}
	return result
}

func (a wal2jsonArguments) toSQLFormat() string {
	var result string
	for i, arg := range a {
		if i != 0 {
			result += ","
		}
		result += fmt.Sprintf("'%s', '%s'", arg.name, arg.value)
	}
	return result
}

func addTablesList(config *PgSource, objects *model.DataObjects, dbLogSnapshot bool) ([]abstract.TableID, error) {
	sourceIncludeTableIDs := make([]abstract.TableID, 0, len(config.DBTables))
	for _, directive := range config.DBTables {
		parsedDirective, err := abstract.ParseTableID(directive)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse table inclusion directive '%s' defined in source endpoint: %w", directive, err)
		}
		sourceIncludeTableIDs = append(sourceIncludeTableIDs, *parsedDirective)
	}
	transferIncludeTableIDs := make([]abstract.TableID, 0, len(objects.GetIncludeObjects()))
	for _, directive := range objects.GetIncludeObjects() {
		parsedDirective, err := abstract.ParseTableID(directive)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse table inclusion directive '%s' defined in transfer: %w", directive, err)
		}
		transferIncludeTableIDs = append(transferIncludeTableIDs, *parsedDirective)
	}
	if len(sourceIncludeTableIDs) == 0 && len(transferIncludeTableIDs) == 0 {
		return nil, nil
	}
	result := abstract.TableIDsIntersection(sourceIncludeTableIDs, transferIncludeTableIDs)
	if len(result) == 0 {
		return nil, xerrors.Errorf("no tables are included in transfer according to specifications of the source endpoint and the transfer")
	}

	consumerKeeperID := *abstract.NewTableID(config.KeeperSchema, TableConsumerKeeper)
	mustAddConsumerKeeper := true
	signalTableID := *dblog.SignalTableTableID(config.KeeperSchema)
	mustAddsignalTable := dbLogSnapshot // the only case when we need to add signalTable into replication - snapshot stage when dblog turned-on

	for _, t := range result {
		if mustAddConsumerKeeper && t.Equals(consumerKeeperID) {
			mustAddConsumerKeeper = false
		}
		if mustAddsignalTable && t.Equals(signalTableID) { // signalTable already added by user - strage, but ok - then we dont need to add it one more time
			mustAddsignalTable = false
		}
	}
	if mustAddConsumerKeeper {
		result = append(result, consumerKeeperID)
	}
	if mustAddsignalTable {
		result = append(result, signalTableID)
	}

	// since inherit table appear dynamically we need to filter tables on our side instead of push-list to postgres
	if config.CollapseInheritTables && objects != nil && len(objects.IncludeObjects) > 0 {
		return nil, nil
	}
	return result, nil
}

func filterTablesList(config *PgSource) ([]abstract.TableID, error) {
	excludeDirectives := config.ExcludeWithGlobals()
	result := make([]abstract.TableID, 0, len(excludeDirectives))
	for _, directive := range excludeDirectives {
		parsedDirective, err := abstract.ParseTableID(directive)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse table exclusion directive '%s' defined in source endpoint: %w", directive, err)
		}
		result = append(result, *parsedDirective)
	}
	return result, nil
}

// wal2jsonTableFromTableID formats TableID so that it can be passed as a parameter to wal2json.
// The specification is at https://github.com/eulerto/wal2json/blob/821147b21cb3672d8c67f708440ff4732e320e0e/README.md#parameters, `filter-tables`
func wal2jsonTableFromTableID(tableID abstract.TableID) string {
	parts := make([]string, 0, 2)
	if len(tableID.Namespace) > 0 {
		parts = append(parts, wal2jsonEscape(tableID.Namespace))
	} else {
		parts = append(parts, "*")
	}
	if len(tableID.Name) > 0 && tableID.Name != "*" {
		parts = append(parts, wal2jsonEscape(tableID.Name))
	} else {
		parts = append(parts, "*")
	}
	return strings.Join(parts, ".")
}

func wal2jsonEscape(identifier string) string {
	builder := strings.Builder{}
	for _, r := range identifier {
		switch r {
		case ' ', '\'', ',', '.', '*':
			_, _ = builder.WriteRune('\\')
		default:
			// do not escape
		}
		_, _ = builder.WriteRune(r)
	}
	return builder.String()
}

// wal2jsonTableList converts the given list of FQTNs into wal2json format
func wal2jsonTableList(tableIDs []abstract.TableID) string {
	parts := make([]string, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		parts = append(parts, wal2jsonTableFromTableID(tableID))
	}
	return strings.Join(parts, ",")
}

func truncate(s string, n int) string {
	if n > len(s) {
		n = len(s)
	}
	return s[:n]
}

func assignKeeperLag(changes []*Wal2JSONItem, slotID string, keeperTime time.Time) time.Time {
	for _, change := range changes {
		if change.Table == TableConsumerKeeper && change.ColumnValues[0] == slotID {
			if val, ok := change.ColumnValues[1].(time.Time); ok {
				keeperTime = val
			}
		}
		change.CommitTime = uint64(keeperTime.UnixNano())
	}
	return keeperTime
}

func walDataSample(data []byte) string {
	const maxSampleLength = 4096
	if len(data) < maxSampleLength {
		return string(data)
	}
	return string(data[:maxSampleLength])
}

func newWalSource(config *PgSource, objects *model.DataObjects, transferID string, registry *stats.SourceStats, lgr log.Logger, slot AbstractSlot, cp coordinator.Coordinator, dbLogSnapshot bool) (abstract.Source, error) {
	connConfig, err := MakeConnConfigFromSrc(lgr, config)
	if err != nil {
		return nil, xerrors.Errorf("error making connection config from pg endpoint params: %w", err)
	}
	lgr.Infof("Trying to create WAL source for master host '%s'", connConfig.Host)

	version, err := resolveVersion(connConfig.Copy())
	if err != nil {
		return nil, xerrors.Errorf("error resolving pg version: %w", err)
	}

	wal2jsonArgs, err := newWal2jsonArguments(config, objects, dbLogSnapshot)
	if err != nil {
		return nil, xerrors.Errorf("failed to build wal2json arguments: %w", err)
	}

	// Polling is workaround for cases where replication connection is not available for some reason
	// There should be no such cases for managed clusters
	canFallbackToPolling := config.ClusterID == ""
	if !config.UsePolling {
		publisher, err := newReplicationPublisher(version, connConfig, slot, wal2jsonArgs, registry, config, transferID, lgr, cp, objects)
		if err == nil {
			return publisher, nil
		}
		if !canFallbackToPolling {
			return nil, xerrors.Errorf("unable to use pg logical replication: %w", err)
		}
		lgr.Warn("Cannot establish replication connection; falling back to polling", log.Error(err))
	}

	return newPollingPublisher(version, connConfig, slot, wal2jsonArgs, registry, config, objects, transferID, lgr, cp)
}

func resolveVersion(connConfig *pgx.ConnConfig) (PgVersion, error) {
	var version PgVersion
	// Version resolve queries may fail and it's not a problem so no reason to log such queries
	connPoolWithoutLogger, err := NewPgConnPool(connConfig, nil)
	if err != nil {
		return version, err
	}
	defer connPoolWithoutLogger.Close()
	return ResolveVersion(connPoolWithoutLogger), nil
}

func validateChangeItemsPtrs(wal2jsonItems []*Wal2JSONItem) error {
	if wal2jsonItems == nil {
		return nil
	}
	for _, wal2jsonItem := range wal2jsonItems {
		if len(wal2jsonItem.ColumnNames) != len(wal2jsonItem.ColumnTypeOIDs) {
			return xerrors.Errorf("column and OID counts differ; columns: %v; oids: %v", wal2jsonItem.ColumnNames, wal2jsonItem.ColumnTypeOIDs)
		}
		if len(wal2jsonItem.OldKeys.KeyNames) != len(wal2jsonItem.OldKeys.KeyTypeOids) {
			return xerrors.Errorf("column and OID counts differ; columns: %v; oids: %v", wal2jsonItem.ColumnNames, wal2jsonItem.ColumnTypeOIDs)
		}
	}
	return nil
}
