//go:build !disable_mysql_provider

package mysql

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gobwrapper.RegisterName("*server.MysqlSource", new(MysqlSource))
	gobwrapper.RegisterName("*server.MysqlDestination", new(MysqlDestination))
	model.RegisterDestination(ProviderType, func() model.Destination {
		return new(MysqlDestination)
	})
	model.RegisterSource(ProviderType, func() model.Source {
		return new(MysqlSource)
	})

	abstract.RegisterProviderName(ProviderType, "MySQL")
	providers.Register(ProviderType, New)

	/*
		"__table_transfer_progress":
			{name VARCHAR(255), lsn BIGINT, status ENUM('SnapshotWait', 'SyncWait', 'InSync')}
			Table (in target-DB) needed for resolving data overlapping during SNAPSHOT_AND_INCREMENT transfers.

		"__tm_keeper":
			{server_id VARCHAR(100), file VARCHAR(1000), pos INTEGER}
			Table for storing current position in binlog for each transfer (by unique server_id of endpoint).

		"__tm_gtid_keeper":
			{server_id VARCHAR(100), host VARCHAR(100), gtid VARCHAR(1000), flavor VARCHAR(100)}
			Table for saving Global Transaction IDs.
	*/
	abstract.RegisterSystemTables(TableTransferProgress, TableTmGtidKeeper, TableTmKeeper)
}

const (
	TableTransferProgress = "__table_transfer_progress"
	TableTmGtidKeeper     = "__tm_gtid_keeper"
	TableTmKeeper         = "__tm_keeper"
)

func isSystemTable(tableName string) bool {
	switch tableName {
	case TableTransferProgress, TableTmGtidKeeper, TableTmKeeper:
		return true
	}
	return false
}

// To verify providers contract implementation
var (
	_ providers.Snapshot    = (*Provider)(nil)
	_ providers.Replication = (*Provider)(nil)
	_ providers.Sinker      = (*Provider)(nil)
	_ providers.Sampleable  = (*Provider)(nil)

	_ providers.Activator   = (*Provider)(nil)
	_ providers.Deactivator = (*Provider)(nil)
	_ providers.Cleanuper   = (*Provider)(nil)
	_ providers.Updater     = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) SourceSampleableStorage() (abstract.SampleableStorage, []abstract.TableDescription, error) {
	src, ok := p.transfer.Src.(*MysqlSource)
	if !ok {
		return nil, nil, xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	srcStorage, err := NewStorage(src.ToStorageParams())
	if err != nil {
		return nil, nil, xerrors.Errorf(`unable to create mysql storage: %w`, err)
	}
	if _, ok := p.transfer.Dst.(*MysqlDestination); ok {
		// Need to toggle is homo to prevent rows to YT types conversions
		srcStorage.IsHomo = true
	}
	all, err := srcStorage.TableList(nil)
	if err != nil {
		return nil, nil, xerrors.Errorf(`unable to get table map from source storage: %w`, err)
	}
	var tables []abstract.TableDescription
	for tID, tInfo := range all {
		if isSystemTable(tID.Name) {
			continue
		}
		if src.Include(tID) {
			tables = append(tables, abstract.TableDescription{
				Name:   tID.Name,
				Schema: tID.Namespace,
				Filter: "",
				EtaRow: tInfo.EtaRow,
				Offset: 0,
			})
		}
	}
	return srcStorage, tables, nil
}

func (p *Provider) DestinationSampleableStorage() (abstract.SampleableStorage, error) {
	dst, ok := p.transfer.Dst.(*MysqlDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	dstStorage, err := NewStorage(dst.ToStorageParams())
	if err != nil {
		return nil, xerrors.Errorf(`unable to create mysql storage: %w`, err)
	}
	if _, ok := p.transfer.Src.(*MysqlSource); ok {
		// Need to toggle is homo to prevent rows to YT types conversions
		dstStorage.IsHomo = true
	}
	return dstStorage, nil
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*MysqlSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	if !src.IsHomo {
		src.IsHomo = p.transfer.DstType() == ProviderType && !src.PlzNoHomo
	}
	if serializer, ok := p.transfer.Dst.(model.Serializable); ok {
		serializationFormat, _ := serializer.Serializer()
		if serializationFormat.Name == model.SerializationFormatDebezium {
			timeZone := debeziumparameters.GetMysqlTimeZone(serializationFormat.Settings)
			src.Timezone = timeZone
		}
	}
	res, err := NewStorage(src.ToStorageParams())
	if err != nil {
		return nil, xerrors.Errorf("unable to construct storage: %w", err)
	}
	res.IsHomo = src.IsHomo
	return res, nil
}

func (p *Provider) Source() (abstract.Source, error) {
	var res abstract.Source
	src, ok := p.transfer.Src.(*MysqlSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	if !src.IsHomo {
		src.IsHomo = p.transfer.DstType() == ProviderType && !src.PlzNoHomo
	}
	src.InitServerID(p.transfer.ID)
	// See TM-4581
	failOnDecimal := isFailOnDecimal(p.transfer)
	if serializer, ok := p.transfer.Dst.(model.Serializable); ok {
		serializationFormat, _ := serializer.Serializer()
		if serializationFormat.Name == model.SerializationFormatDebezium {
			timeZone := debeziumparameters.GetMysqlTimeZone(serializationFormat.Settings)
			src.Timezone = timeZone
		}
	}
	if err := backoff.Retry(func() error {
		if source, err := NewSource(src, p.transfer.ID, p.transfer.DataObjects, p.logger, p.registry, p.cp, failOnDecimal); err != nil {
			return xerrors.Errorf("unable to create new mysql source: %w", err)
		} else {
			res = source
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)); err != nil {
		return nil, err
	}
	return res, nil
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*MysqlDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return NewSinker(p.logger, dst, p.registry)
}

func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	registry := solomon.NewRegistry(solomon.NewRegistryOpts())
	src, ok := p.transfer.Src.(*MysqlSource)
	if !ok {
		return xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	if err := checkRestrictedColumnTypes(p.transfer, tables); err != nil {
		return xerrors.Errorf("Column of the restricted type found: %w", err)
	}
	if p.transfer.SrcType() == p.transfer.DstType() {
		// mysql treat views as metadata, so no need to transfer them separately
		model.ExcludeViews(tables)
	}
	if !p.transfer.SnapshotOnly() {
		if src.IsHomo {
			if err := CheckMySQLBinlogRowImageFormat(src); err != nil {
				return err
			}
		}
		src.InitServerID(p.transfer.ID)
		if err := SyncBinlogPosition(src, p.transfer.ID, p.cp); err != nil {
			return xerrors.Errorf("Cannot synchronize binlog position: %w", err)
		}

	}
	if !p.transfer.IncrementOnly() {
		if err := callbacks.Cleanup(tables); err != nil {
			return xerrors.Errorf("Sinker cleanup failed: %w", err)
		}
		if err := LoadMysqlSchema(p.transfer, registry, false); err != nil {
			return xerrors.Errorf("Cannot load schema from source database: %w", err)
		}

		if err := callbacks.CheckIncludes(tables); err != nil {
			return xerrors.Errorf("Failed in accordance with configuration: %w", err)
		}
		if err := callbacks.Upload(tables); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
		if p.transfer.SnapshotOnly() {
			if err := LoadMysqlSchema(p.transfer, registry, true); err != nil {
				return xerrors.Errorf("Cannot load schema from source database: %w", err)
			}
		}
	}
	return nil
}

func (p *Provider) Deactivate(ctx context.Context, task *model.TransferOperation) error {
	if p.transfer.SnapshotOnly() {
		return nil
	}
	src, ok := p.transfer.Src.(*MysqlSource)
	if !ok {
		return xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	if err := RemoveTracker(src, p.transfer.ID, p.cp); err != nil {
		return xerrors.Errorf("Unable to remove tracker: %w", err)
	}
	if !p.transfer.IncrementOnly() {
		if err := LoadMysqlSchema(p.transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), true); err != nil {
			return xerrors.Errorf("Cannot load schema from source database: %w", err)
		}
	}
	return nil
}

func (p *Provider) Cleanup(ctx context.Context, task *model.TransferOperation) error {
	src, ok := p.transfer.Src.(*MysqlSource)
	if !ok {
		return xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	return RemoveTracker(src, p.transfer.ID, p.cp)
}

func (p *Provider) Update(ctx context.Context, addedTables []abstract.TableDescription) error {
	return LoadMysqlSchema(p.transfer, p.registry, false)
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
