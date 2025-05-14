package greenplum

import (
	"context"
	"net"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/greenplum/gpfdist"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"go.ytsaurus.tech/library/go/core/log"
)

const pushBatchSize = 1000

var _ abstract.Storage = (*GpfdistStorage)(nil)

type GpfdistStorage struct {
	*Storage
	src *GpSource
}

func (s *GpfdistStorage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	schema, err := s.TableSchema(ctx, table.ID())
	if err != nil {
		return xerrors.Errorf("unable to retrive table schema: %w", err)
	}

	conn, err := coordinatorConnFromStorage(s.Storage)
	if err != nil {
		return xerrors.Errorf("unable to init coordinator conn: %w", err)
	}
	localAddr, err := localAddrFromStorage(s.Storage)
	if err != nil {
		return xerrors.Errorf("unable to get local address: %w", err)
	}
	gpfd, err := gpfdistbin.InitGpfdist(s.src.GpfdistParams, localAddr, gpfdistbin.ExportTable, conn)
	if err != nil {
		return xerrors.Errorf("unable to init gpfdist: %w", err)
	}
	logger.Log.Debugf("Gpfdist for storage initialized")
	defer func() {
		if err := gpfd.Stop(); err != nil {
			logger.Log.Error("Unable to stop gpfdist", log.Error(err))
		}
	}()

	// Async run PipesReader which will parse data from pipes and push it.
	pipeReader := gpfdist.NewPipeReader(gpfd, s.itemTemplate(table, schema), pushBatchSize)
	go pipeReader.Run(pusher)

	// Run gpfdist export through external table.
	extTableRows, err := gpfd.RunExternalTableTransaction(ctx, table.ID(), schema)
	if err != nil {
		return xerrors.Errorf("unable to create external table and insert rows: %w", err)
	}

	pipeReaderRows, pipeReaderErr := pipeReader.Stop(10 * time.Minute)
	if pipeReaderErr != nil {
		return xerrors.Errorf("unable to read pipes and push rows: %w", pipeReaderErr)
	}
	if extTableRows != pipeReaderRows {
		return xerrors.Errorf("to pipe pushed %d rows, while to external table - %d", pipeReaderRows, extTableRows)
	}
	return nil
}

func (s *GpfdistStorage) itemTemplate(table abstract.TableDescription, schema *abstract.TableSchema) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:           uint32(0),
		LSN:          uint64(0),
		CommitTime:   uint64(time.Now().UTC().UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       table.Schema,
		Table:        table.Name,
		PartID:       table.PartID(),
		ColumnNames:  schema.Columns().ColumnNames(),
		ColumnValues: nil,
		TableSchema:  schema,
		OldKeys:      abstract.EmptyOldKeys(),
		TxID:         "",
		Query:        "",
		Size:         abstract.EmptyEventSize(),
	}
}

func coordinatorConnFromStorage(storage *Storage) (*pgxpool.Pool, error) {
	coordinator, err := storage.PGStorage(context.Background(), Coordinator())
	return coordinator.Conn, err
}

// localAddrFromStorage returns host for external connections (from GreenPlum VMs to Transfer VMs).
func localAddrFromStorage(storage *Storage) (net.IP, error) {
	var gpAddr *GpHP
	var err error
	if storage.config.MDBClusterID() != "" {
		if gpAddr, _, err = storage.ResolveDbaasMasterHosts(); err != nil {
			return nil, xerrors.Errorf("unable to resolve dbaas master host: %w", err)
		}
	} else {
		if gpAddr, err = storage.config.Connection.OnPremises.Coordinator.AnyAvailable(); err != nil {
			return nil, xerrors.Errorf("unable to get coordinator host: %w", err)
		}
	}

	conn, err := net.Dial("tcp", gpAddr.String())
	if err != nil {
		return nil, xerrors.Errorf("unable to dial GP address %s: %w", gpAddr, err)
	}
	defer conn.Close()

	addr := conn.LocalAddr()
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return nil, xerrors.Errorf("expected LocalAddr to be *net.TCPAddr, got %T", addr)
	}
	return tcpAddr.IP, nil
}

func NewGpfdistStorage(config *GpSource, mRegistry metrics.Registry) (*GpfdistStorage, error) {
	return &GpfdistStorage{
		Storage: NewStorage(config, mRegistry),
		src:     config,
	}, nil
}
