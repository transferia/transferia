package greenplum

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/providers/greenplum/gpfdist"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

const pushBatchSize = 10000

var _ abstract.Storage = (*GpfdistStorage)(nil)

type GpfdistStorage struct {
	storage *Storage
	src     *GpSource
	params  gpfdistbin.GpfdistParams
}

func NewGpfdistStorage(src *GpSource, mRegistry metrics.Registry, params gpfdistbin.GpfdistParams) *GpfdistStorage {
	return &GpfdistStorage{
		storage: NewStorage(src, mRegistry),
		src:     src,
		params:  params,
	}
}

func (s *GpfdistStorage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	schema, err := s.TableSchema(ctx, table.ID())
	if err != nil {
		return xerrors.Errorf("unable to retrive table schema: %w", err)
	}

	conn, err := coordinatorConnFromStorage(s.storage)
	if err != nil {
		return xerrors.Errorf("unable to init coordinator conn: %w", err)
	}
	localAddr, err := localAddrFromStorage(s.storage)
	if err != nil {
		return xerrors.Errorf("unable to get local address: %w", err)
	}
	mode := gpfdistbin.ExportTable

	// Step 1. Run gpfdists and PipeReaders.
	if s.params.ThreadsCount <= 0 {
		return xerrors.Errorf("gpfdist parallel setting (%d) should be positive", s.params.ThreadsCount)
	}
	gpfdists := make([]*gpfdistbin.Gpfdist, s.params.ThreadsCount)
	locations := make([]string, s.params.ThreadsCount)
	pipeReaders := make([]*gpfdist.PipeReader, s.params.ThreadsCount)
	for i := range gpfdists {
		gpfdists[i], err = gpfdistbin.InitGpfdist(s.params, localAddr, mode, i)
		if err != nil {
			return xerrors.Errorf("unable to init gpfdist #%d: %w", i, err)
		}
		locations[i] = gpfdists[i].Location()
		// Async run PipesReader which will parse data from pipes and push it.
		pipeReaders[i] = gpfdist.NewPipeReader(gpfdists[i], itemTemplate(table, schema), pushBatchSize)
		go pipeReaders[i].Run(pusher)
	}
	logger.Log.Debugf("%d gpfdists for storage initialized", len(gpfdists))

	defer func() {
		for _, gpfd := range gpfdists {
			if err := gpfd.Stop(); err != nil {
				logger.Log.Error("Unable to stop gpfdist", log.Error(err))
			}
		}
	}()

	// Step 2. Run gpfdist export through external table.
	ddlExecutor := gpfdistbin.NewGpfdistDDLExecutor(conn, s.params.ServiceSchema)
	extRows, err := ddlExecutor.RunExternalTableTransaction(
		ctx, mode.ToExternalTableMode(), table.ID(), schema, locations,
	)
	if err != nil {
		return xerrors.Errorf("unable to create external table and insert rows: %w", err)
	}

	// Step 3. Close PipeReaders and check that their rows count is equal to external table rows count.
	pipeRows := atomic.Int64{}
	eg := errgroup.Group{}
	for _, pipeReader := range pipeReaders {
		eg.Go(func() error {
			rows, err := pipeReader.Stop(10 * time.Minute)
			pipeRows.Add(rows)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return xerrors.Errorf("unable to read pipes and push rows: %w", err)
	}
	if extRows != pipeRows.Load() {
		return xerrors.Errorf("to pipe pushed %d rows, to external table - %d", pipeRows.Load(), extRows)
	}
	return nil
}

func itemTemplate(table abstract.TableDescription, schema *abstract.TableSchema) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:               uint32(0),
		LSN:              uint64(0),
		CommitTime:       uint64(time.Now().UTC().UnixNano()),
		Counter:          0,
		Kind:             abstract.InsertKind,
		Schema:           table.Schema,
		Table:            table.Name,
		PartID:           table.PartID(),
		ColumnNames:      schema.Columns().ColumnNames(),
		ColumnValues:     nil,
		TableSchema:      schema,
		OldKeys:          abstract.EmptyOldKeys(),
		Size:             abstract.EmptyEventSize(),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
}

func coordinatorConnFromStorage(storage *Storage) (*pgxpool.Pool, error) {
	coordinator, err := storage.PGStorage(context.Background(), Coordinator())
	if err != nil {
		return nil, err
	}
	return coordinator.Conn, nil
}

func getLocalIP() (net.IP, error) {
	var res []net.IP
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 || !strings.HasPrefix(iface.Name, "eth") {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() || !ip.To4().IsGlobalUnicast() {
				continue // Skip IPv6, loopback and link-local addresses.
			}
			res = append(res, ip)
		}
	}
	if len(res) > 0 {
		return res[0], nil
	}
	return nil, fmt.Errorf("no non-loopback, unicast IPv4 address found")
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
	if tcpAddr.IP.IsLoopback() {
		logger.Log.Warnf("Dial local address is loopback (%s), resolving from interfaces", tcpAddr.IP.String())
		return getLocalIP()
	}
	return tcpAddr.IP, nil
}

func (s *GpfdistStorage) Close() { s.storage.Close() }

func (s *GpfdistStorage) Ping() error { return s.storage.Ping() }

func (s *GpfdistStorage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.storage.TableSchema(ctx, table)
}

func (s *GpfdistStorage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	return s.storage.TableList(filter)
}

func (s *GpfdistStorage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.storage.ExactTableRowsCount(table)
}

func (s *GpfdistStorage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.storage.EstimateTableRowsCount(table)
}

func (s *GpfdistStorage) TableExists(table abstract.TableID) (bool, error) {
	return s.storage.TableExists(table)
}
