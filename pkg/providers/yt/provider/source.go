package provider

import (
	"context"
	"strings"
	"sync"
	"time"

	gofrs_uuid "github.com/gofrs/uuid"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/cypressmeta"
	"github.com/transferia/transferia/pkg/providers/yt/provider/dataobjects"
	yt_provider_schema "github.com/transferia/transferia/pkg/providers/yt/provider/schema"
	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

// mainTxTimeout is the timeout of the snapshot TX. It only holds a snapshot
// lock and never mutates data, so a relatively short timeout is fine — the
// TX is refreshed by every YT call inside BeginSnapshot/EndSnapshot window.
var mainTxTimeout = yson.Duration(10 * time.Minute)

// source implements abstract.Storage over a YT cluster: it enumerates tables
// under the configured cfg.GetPaths(), locks them in a snapshot TX, and
// serves rows through Skiff-based parallel readers.
type source struct {
	cfg            provider_yt.YtSourceModel
	yt             yt.Client
	tx             yt.Tx
	txID           yt.TxID
	logger         log.Logger
	metrics        *stats.SourceStats
	columnFilter   map[yt.NodeID][]string
	includeObjects []string

	// mu guards tx, nodes and partsMapping: LoadTable, ShardTable and
	// TableList run concurrently on the same instance.
	mu           sync.Mutex
	nodes        cypressmeta.YtNodes // listing cache, valid for one snapshot
	partsMapping map[string]int      // global shard budget: tableIDFor(node).Name -> shard count
	txOwned      bool                // tx was started by this instance (main worker), not attached
}

// Compile-time assertions of the storage contracts this source satisfies.
var _ abstract.Storage = (*source)(nil)
var _ abstract.ShardingStorage = (*source)(nil)
var _ abstract.SnapshotableStorage = (*source)(nil)
var _ abstract.ShardingContextStorage = (*source)(nil)
var _ model.FilteredTableLister = (*source)(nil)

// NewSource constructs a v1 abstract.Storage-compatible source.
//
// The include argument carries the transfer include objects
// (DataObjects.IncludeObjects): each entry is a YPath with an optional
// #columns selector understood by ypath.Parse. They double as the listing
// roots for table discovery, so TableID names stay relative to the paths
// the user configured.
func NewSource(logger log.Logger, registry core_metrics.Registry, cfg provider_yt.YtSourceModel, include []string) (*source, error) {
	ytc, err := yt_client.FromConnParams(cfg, logger)
	if err != nil {
		return nil, xerrors.Errorf("unable to create yt client: %w", err)
	}

	//nolint:exhaustivestruct
	return &source{
		cfg:            cfg,
		yt:             ytc,
		tx:             nil,
		txID:           yt.TxID(gofrs_uuid.Nil),
		logger:         logger,
		metrics:        stats.NewSourceStats(registry),
		includeObjects: include,
	}, nil
}

// buildColumnFilter resolves each user-supplied YPath to a NodeID and stores
// the column selector attached to that path. Built lazily by listTables on the
// first listing, so storage construction makes no YT calls and include-path
// errors surface during listing — matching the legacy flow. Column-selector
// state is used purely as a hint, so a slightly stale NodeID lookup is
// acceptable.
func buildColumnFilter(client yt.Client, include []string) (map[yt.NodeID][]string, error) {
	res := make(map[yt.NodeID][]string)

	for _, object := range include {
		yp, err := ypath.Parse(object)
		if err != nil {
			return nil, xerrors.Errorf("cannot parse input ypath %s: %w", object, err)
		}
		var nodeID yt.NodeID
		if err := client.GetNode(context.Background(), yp.YPath().Attr("id"), &nodeID, nil); err != nil {
			return nil, xerrors.Errorf("cannot get node id for table %s: %w", object, err)
		}
		res[nodeID] = yp.Columns
	}
	return res, nil
}

// --- abstract.Storage ---

func (s *source) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// An attached TX (secondary worker) is never aborted here: the TX is
	// owned by the main worker, which aborts it in EndSnapshot after all
	// secondaries finish. The yt-go Tx interface has no Detach(): aborting
	// any handle aborts the shared master TX, and cancelling the attach
	// context triggers a background abort in the client pinger. So the only
	// safe cleanup is dropping the reference — the auto-ping goroutine exits
	// by itself when the main worker finishes the TX.
	if !s.txOwned {
		s.tx = nil
		return
	}
	// Abort the snapshot TX if it was never ended via EndSnapshot (e.g.
	// test_endpoint paths where Close runs in a defer).
	if s.tx != nil {
		if err := s.tx.Abort(); err != nil {
			s.logger.Warn("Error aborting YT snapshot TX in Close", log.Error(err))
		}
		s.tx = nil
		s.txOwned = false
		s.nodes = nil
		s.partsMapping = nil
	}
}

func (s *source) Ping() error {
	return nil
}

func (s *source) TableSchema(ctx context.Context, tid abstract.TableID) (*abstract.TableSchema, error) {
	node, err := s.resolveTable(ctx, tid)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve table %s: %w", tid.Fqtn(), err)
	}
	if node.NodeID != nil {
		yttable, err := resolveYtTable(ctx, s.yt, s.txID, *node.NodeID, node.Name, s.columnsFor(*node.NodeID), s.cfg.GetRowIdxColumn())
		if err != nil {
			return nil, xerrors.Errorf("unable to load yt schema: %w", err)
		}
		tbl, err := yttable.ToOldTable()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert table to old format: %w", err)
		}
		return tbl, nil
	}
	// NodeID is only set when a snapshot TX locked the table. Secondary
	// workers receive the TX id through SetShardingContext but never call
	// BeginSnapshot themselves, so they fall back to path-based lookup.
	yttable, err := resolveYtTableByPath(ctx, s.yt, s.txID, ypath.Path(node.OriginalPath()), node.Name, nil, s.cfg.GetRowIdxColumn())
	if err != nil {
		return nil, xerrors.Errorf("unable to load yt schema by path: %w", err)
	}
	tbl, err := yttable.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("unable to convert table to old format: %w", err)
	}
	return tbl, nil
}

func (s *source) TableList(includeFilter abstract.IncludeTableList) (abstract.TableMap, error) {
	ctx := context.Background()
	tables, err := s.listTables(ctx)
	if err != nil {
		return nil, xerrors.Errorf("unable to list tables: %w", err)
	}

	tables, err = uniqueTableIDs(tables)
	if err != nil {
		return nil, xerrors.Errorf("unable to list tables: %w", err)
	}

	res := make(abstract.TableMap, len(tables))
	for _, t := range tables {
		tid := tableIDFor(t)
		sch, err := s.loadTableSchema(ctx, t)
		if err != nil {
			return nil, xerrors.Errorf("unable to load yt schema for %s: %w", t.OriginalPath(), err)
		}
		res[tid] = abstract.TableInfo{
			EtaRow: uint64(t.RowCount),
			IsView: false,
			Schema: sch,
		}
	}

	return model.FilteredMap(res, includeFilter), nil
}

// FilteredTableList implements model.FilteredTableLister. It lists YT nodes and
// filters by include directives in a single pass. When include objects are set
// they are used as the listing paths (more efficient than listing all source
// paths). Directory-style includes match all tables underneath via prefix
// comparison on OriginalPath.
func (s *source) FilteredTableList(transfer *model.Transfer) (abstract.TableMap, error) {
	ctx := context.Background()

	var includeObjects []string
	if transfer.DataObjects != nil {
		includeObjects = transfer.DataObjects.IncludeObjects
	}

	// Listing goes through listTables: it opens/attaches the snapshot TX,
	// fills in NodeIDs via snapshot locks and caches the result, so schema
	// reads below apply the column filter (P1#3) and the listing happens
	// exactly once per snapshot (P1#7).
	nodes, err := s.listTables(ctx)
	if err != nil {
		return nil, xerrors.Errorf("unable to list yt nodes: %w", err)
	}

	filtered, err := s.filterByIncludes(ctx, nodes, includeObjects)
	if err != nil {
		return nil, err
	}

	// Collision detection runs on the include-filtered set: an include that
	// selects only one of two same-named tables must not fail the snapshot.
	filtered, err = uniqueTableIDs(filtered)
	if err != nil {
		return nil, xerrors.Errorf("unable to list yt nodes: %w", err)
	}

	res := make(abstract.TableMap, len(filtered))
	for _, node := range filtered {
		tid := tableIDFor(node)
		sch, err := s.loadTableSchema(ctx, node)
		if err != nil {
			return nil, xerrors.Errorf("unable to load yt schema for %s: %w", node.OriginalPath(), err)
		}
		res[tid] = abstract.TableInfo{
			EtaRow: uint64(node.RowCount),
			IsView: false,
			Schema: sch,
		}
	}
	return res, nil
}

// filterByIncludes returns nodes whose OriginalPath matches at least one
// include object (exact or prefix match for directories). Every include
// object must match at least one node; an error is returned otherwise.
//
// Nodes are iterated in listing order so that for overlapping roots the first
// occurrence wins — the same winner uniqueTableIDs used to pick.
func (s *source) filterByIncludes(ctx context.Context, nodes cypressmeta.YtNodes, includeObjects []string) (cypressmeta.YtNodes, error) {
	if len(includeObjects) == 0 {
		return nodes, nil
	}

	cleanPaths := make(map[string]string, len(includeObjects))
	for _, obj := range includeObjects {
		cleanPath := obj
		if yp, err := ypath.Parse(obj); err == nil {
			cleanPath = yp.YPath().String()
		}
		cleanPaths[obj] = cleanPath
	}
	matches := func(path string) bool {
		for _, cleanPath := range cleanPaths {
			if path == cleanPath || strings.HasPrefix(path, cleanPath+"/") {
				return true
			}
		}
		return false
	}

	found := make(map[string]bool, len(includeObjects))
	seen := make(map[string]bool)
	var filtered cypressmeta.YtNodes
	for _, node := range nodes {
		path := node.OriginalPath()
		if !matches(path) || seen[path] {
			continue
		}
		seen[path] = true
		filtered = append(filtered, node)
		for obj, cleanPath := range cleanPaths {
			if path == cleanPath || strings.HasPrefix(path, cleanPath+"/") {
				found[obj] = true
			}
		}
	}
	for _, obj := range includeObjects {
		if found[obj] {
			continue
		}
		// Distinguish "object does not exist" (fail, like the trunk listing did)
		// from "object exists but has no tables under it" (skip silently —
		// the legacy filter contributed nothing for such includes).
		objPath := obj
		if yp, err := ypath.Parse(obj); err == nil {
			objPath = yp.YPath().String()
		}
		var nodeID yt.NodeID
		if err := s.yt.GetNode(ctx, ypath.Path(objPath).Attr("id"), &nodeID, nil); err != nil {
			return nil, errors.CategorizedErrorf(categories.Source, "object: %s not found in source: %w", obj, err)
		}
	}
	return filtered, nil
}

// loadTableSchema loads a table's schema, either through the active snapshot TX
// (when NodeID is populated from the tx-lock) or directly by path when no
// snapshot is active (preflight TableList before BeginSnapshot). Both branches
// go through the same enrichment pipeline (resolveYtTable / resolveYtTableByPath)
// so that provider-configured virtual columns — namely row_idx — end up in the
// schema seen by SnapshotLoader.schemaCache, which in turn feeds MakeInitTableLoad
// and drives the target-side CREATE TABLE. Keeping this in sync with
// snapshotSource.loadPart avoids "no such column" mismatches on INSERT.
func (s *source) loadTableSchema(ctx context.Context, t *cypressmeta.YtNodeMeta) (*abstract.TableSchema, error) {
	idxCol := s.cfg.GetRowIdxColumn()
	if t.NodeID != nil {
		yttable, err := resolveYtTable(ctx, s.yt, s.txID, *t.NodeID, t.Name, s.columnsFor(*t.NodeID), idxCol)
		if err != nil {
			return nil, xerrors.Errorf("failed to resolveYtTable: %w", err)
		}
		tbl, err := yttable.ToOldTable()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert table to old format: %w", err)
		}
		return tbl, nil
	}
	yttable, err := resolveYtTableByPath(ctx, s.yt, s.txID, ypath.Path(t.OriginalPath()), t.Name, nil, idxCol)
	if err != nil {
		return nil, xerrors.Errorf("failed to resolveYtTableByPath: %w", err)
	}
	tbl, err := yttable.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("unable to convert table to old format: %w", err)
	}
	return tbl, nil
}

// tableIDFor returns a TableID for the given node. Name is the node path
// relative to its listing root — the source path or include object the user
// configured — which is exactly what cypressmeta.ListNodes stores in
// YtNodeMeta.Name. Namespace is always empty, matching the abstract2 canon
// behaviour.
func tableIDFor(t *cypressmeta.YtNodeMeta) abstract.TableID {
	return abstract.TableID{Namespace: "", Name: t.Name}
}

// columnsFor returns the column projection recorded for a node. The filter
// map is built lazily by listTables (or received via SetShardingContext), so
// access goes through the instance lock.
func (s *source) columnsFor(nodeID yt.NodeID) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.columnFilter[nodeID]
}

// uniqueTableIDs dedups the same table listed twice under overlapping roots
// (e.g. a source path and its parent directory) — the first occurrence of an
// OriginalPath wins. Two DIFFERENT tables resolving to the same TableID are
// still rejected: such a configuration would silently overwrite one of them
// on the destination.
func uniqueTableIDs(nodes cypressmeta.YtNodes) (cypressmeta.YtNodes, error) {
	res := make(cypressmeta.YtNodes, 0, len(nodes))
	seen := make(map[abstract.TableID]*cypressmeta.YtNodeMeta, len(nodes))
	seenPath := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		path := node.OriginalPath()
		if seenPath[path] {
			continue
		}
		seenPath[path] = true
		tid := tableIDFor(node)
		if prev, ok := seen[tid]; ok {
			return nil, coded.Errorf(
				error_codes.YTTableNameCollision,
				"table name collision: %q and %q both resolve to %q; "+
					"tables with identical relative names under different roots are not supported",
				prev.OriginalPath(), node.OriginalPath(), tid.Name)
		}
		seen[tid] = node
		res = append(res, node)
	}
	return res, nil
}

// resolveYtTable is the single source of truth for how the YT source describes
// a table: the raw YT schema fetched through the caller's snapshot TX plus
// provider-configured virtual columns (currently just row_idx). Every schema
// consumer inside this package — Storage.TableSchema, loadTableSchema (the
// NodeID branch), and snapshotSource.loadPart — funnels through it so init-event
// and data-row schemas cannot drift.
func resolveYtTable(ctx context.Context, ytc yt.Client, txID yt.TxID,
	nodeID yt.NodeID, name string, includeCols []string, idxCol string,
) (yt_table.YtTable, error) {
	tbl, err := yt_provider_schema.Load(ctx, ytc, txID, nodeID, name, includeCols)
	if err != nil {
		return nil, err
	}
	if idxCol != "" {
		yt_provider_schema.AddRowIdxColumn(tbl, idxCol)
	}
	return tbl, nil
}

// resolveYtTableByPath is the path-based twin of resolveYtTable, used when
// no NodeID has been obtained — either in the preflight TableList branch
// (where BeginSnapshot has not been called) or on secondary workers that
// receive the TX id through SetShardingContext but never lock tables
// themselves. The caller MUST supply the snapshot txID so the read sees the
// same snapshot state as the main worker.
func resolveYtTableByPath(ctx context.Context, ytc yt.Client, txID yt.TxID, path ypath.Path,
	name string, includeCols []string, idxCol string,
) (yt_table.YtTable, error) {
	var sch ytschema.Schema
	var opts *yt.GetNodeOptions
	if txID != (yt.TxID{}) {
		opts = &yt.GetNodeOptions{TransactionOptions: &yt.TransactionOptions{TransactionID: txID}}
	}
	if err := ytc.GetNode(ctx, path.Attr("schema"), &sch, opts); err != nil {
		return nil, xerrors.Errorf("unable to fetch schema attr for %s: %w", path.String(), err)
	}
	tbl, err := yt_provider_schema.FromRawSchema(name, sch, includeCols)
	if err != nil {
		return nil, err
	}
	if idxCol != "" {
		yt_provider_schema.AddRowIdxColumn(tbl, idxCol)
	}
	return tbl, nil
}

func (s *source) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	part, err := s.tableDescriptionToPart(ctx, table)
	if err != nil {
		return xerrors.Errorf("unable to build part for %s: %w", table.Fqtn(), err)
	}
	snap := NewSnapshotSource(s.cfg, s.yt, part, s.logger, s.metrics, s.columnsFor(part.NodeID()))
	// Pass the full TableDescription so the batch's ChangeItem.{Schema, Table, PartID}
	// mirror what MakeInitTableLoad in load_snapshot.go emits — the async CH sink
	// keys its part map by TablePartID{TableID, PartID}, so a mismatch on any of
	// those fields makes the pushed rows fail to correlate with the registered part.
	return snap.loadPart(ctx, table, pusher)
}

func (s *source) ExactTableRowsCount(tid abstract.TableID) (uint64, error) {
	node, err := s.resolveTable(context.Background(), tid)
	if err != nil {
		return 0, xerrors.Errorf("unable to resolve table %s: %w", tid.Fqtn(), err)
	}
	return uint64(node.RowCount), nil
}

func (s *source) EstimateTableRowsCount(tid abstract.TableID) (uint64, error) {
	return s.ExactTableRowsCount(tid)
}

func (s *source) TableExists(tid abstract.TableID) (bool, error) {
	tables, err := s.listTables(context.Background())
	if err != nil {
		return false, xerrors.Errorf("unable to list tables: %w", err)
	}
	for _, t := range tables {
		if tableIDFor(t) == tid {
			return true, nil
		}
	}
	return false, nil
}

// --- abstract.SnapshotableStorage ---

func (s *source) BeginSnapshot(ctx context.Context) error {
	// Idempotent: listTables may have already opened the TX via ensureTx
	// (e.g. from FilteredTableList), so a second call must not leak another TX.
	return s.ensureTx(ctx)
}

// ensureTx opens the snapshot TX on first use. Called from listTables, so any
// listing (preflight TableList, FTL, ShardTable, resolveTable) automatically
// holds a snapshot lock from that point on — matching the legacy behaviour
// where the main worker kept the TX open for the whole snapshot. The yt
// client pings the handle in the background (see yt-go internal.Pinger), so
// the lease survives long snapshot windows even when this worker makes no
// further YT calls.
func (s *source) ensureTx(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tx != nil {
		return nil
	}
	tx, err := s.yt.BeginTx(ctx, &yt.StartTxOptions{Timeout: &mainTxTimeout})
	if err != nil {
		return xerrors.Errorf("error starting snapshot TX: %w", err)
	}
	s.tx = tx
	s.txID = tx.ID()
	s.txOwned = true
	return nil
}

func (s *source) EndSnapshot(ctx context.Context) error {
	// Since the only goal of TX is to hold snapshot lock and no data
	// modification should happen, it is safe to ignore any errors; the TX
	// may be already aborted or will be aborted by YT after the transfer ends.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tx == nil {
		return nil
	}
	if s.txOwned {
		if err := s.tx.Abort(); err != nil {
			s.logger.Warn("Error aborting YT snapshot TX", log.Error(err))
		}
		s.txOwned = false
	}
	s.tx = nil
	s.nodes = nil
	s.partsMapping = nil
	return nil
}

// --- abstract.ShardingStorage ---

// ShardTable takes a single table description (typically produced by
// TableList) and expands it into row-range parts sized around
// cfg.GetDesiredPartSizeBytes(). Each returned TableDescription carries a
// YSON PartKey in its Filter — the same on-disk format already-persisted
// plans use, so secondary workers can still parse them via ParsePartKey.
//
// The shard budget is computed once per snapshot across ALL tables (1024
// parts in total, like the legacy global uniformParts), so a single table's
// part count depends on the whole listing.
func (s *source) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	// listTables opens the snapshot TX via ensureTx (or uses the TX attached
	// by SetShardingContext) and returns the cached listing.
	tables, err := s.listTables(ctx)
	if err != nil {
		return nil, xerrors.Errorf("unable to list tables: %w", err)
	}
	var target *cypressmeta.YtNodeMeta
	for _, t := range tables {
		if tableIDFor(t).Name == table.Name {
			target = t
			break
		}
	}
	if target == nil {
		return nil, xerrors.Errorf("table %s not found in configured paths", table.Name)
	}
	if target.RowCount == 0 {
		// Empty tables produce no parts at all — parity with the legacy
		// behaviour. The table stays in the TableMap, so the "no tables in
		// snapshot" guard does not fire.
		return []abstract.TableDescription{}, nil
	}

	shards := 0
	s.mu.Lock()
	if s.partsMapping == nil {
		mapping, err := dataobjects.ComputePartsMapping(tables, s.cfg, s.logger)
		if err != nil {
			s.mu.Unlock()
			return nil, xerrors.Errorf("unable to compute parts mapping: %w", err)
		}
		s.partsMapping = make(map[string]int, len(mapping))
		for i, t := range tables {
			s.partsMapping[tableIDFor(t).Name] = mapping[i]
		}
	}
	shards = s.partsMapping[table.Name]
	s.mu.Unlock()
	if shards == 0 {
		return nil, abstract.NewNonShardableError(
			xerrors.Errorf("table %s has no shards allocated", table.Name))
	}

	s.mu.Lock()
	tx, txID := s.tx, s.txID
	s.mu.Unlock()
	parts, err := dataobjects.BuildPartsForTable(ctx, tx, txID, target, shards)
	if err != nil {
		return nil, xerrors.Errorf("unable to build parts for table %s: %w", table.Name, err)
	}
	if len(parts) <= 1 {
		return nil, abstract.NewNonShardableError(
			xerrors.Errorf("table %s is too small to shard (%d part)", table.Name, len(parts)))
	}
	return parts, nil
}

// --- abstract.ShardingContextStorage ---

// SnapshotState is the on-wire form of the sharding context passed from the
// main worker to secondary workers so they can attach to the same snapshot TX
// and see the same column projections.
type SnapshotState struct {
	TxID         yt.TxID                `yson:"tx_id"`
	ColumnFilter map[yt.NodeID][]string `yson:"column_filter"`
}

func (s *source) ShardingContext() ([]byte, error) {
	s.mu.Lock()
	state := &SnapshotState{
		TxID:         s.txID,
		ColumnFilter: s.columnFilter,
	}
	s.mu.Unlock()
	ysonctx, err := yson.Marshal(state)
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal yt config: %w", err)
	}
	return ysonctx, nil
}

func (s *source) SetShardingContext(shardedState []byte) error {
	res := new(SnapshotState)
	if err := yson.Unmarshal(shardedState, res); err != nil {
		return xerrors.Errorf("unable to unmarshal sharding state for yt: %w", err)
	}
	if res.TxID == (yt.TxID{}) {
		return xerrors.New("empty tx id in yt sharding state")
	}

	// Secondary workers never call BeginSnapshot themselves: they must attach
	// to the main worker's snapshot TX so locks and reads stay on the same
	// snapshot. AutoPingable keeps the TX alive for the duration of the load.
	//
	// The handle is attached with a never-cancelled context and must never be
	// aborted or detached: aborting any handle aborts the shared master TX,
	// and cancelling the attach context triggers a background abort in the yt
	// client. The auto-ping goroutine exits by itself once the main worker
	// finishes the TX in EndSnapshot.
	tx, err := s.yt.AttachTx(context.Background(), res.TxID, &yt.AttachTxOptions{AutoPingable: true})
	if err != nil {
		return xerrors.Errorf("unable to attach to snapshot tx: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// If a listing raced ahead of SetShardingContext, ensureTx opened a local
	// TX. Drop it: this worker must read through the shared snapshot TX only,
	// and aborting the attached handle would kill the master TX.
	if s.txOwned && s.tx != nil {
		if err := s.tx.Abort(); err != nil {
			s.logger.Warn("Error aborting locally-started snapshot TX in SetShardingContext", log.Error(err))
		}
		s.txOwned = false
		// The listing cache and shard budget were populated through the
		// aborted local TX; drop them so the next listTables rebuilds them
		// through the attached snapshot TX.
		s.nodes = nil
		s.partsMapping = nil
	}
	s.tx = tx
	s.txID = res.TxID
	s.columnFilter = res.ColumnFilter
	return nil
}

// --- internal helpers ---

// listPaths returns the roots used for listing tables: the include objects
// when present, otherwise the configured source paths. YtNodeMeta.Name (and
// therefore TableID.Name) is relative to these roots, so every listing in
// this source must use the same set — otherwise lookups like ShardTable or
// resolveTable cannot find tables whose names came from another root.
//
// Unlike the legacy flow, include objects outside cfg.GetPaths() are NOT
// rejected here ("unable to find %s in source"): includes double as the
// listing roots, so anything outside the configured paths is simply listed
// directly. This is a deliberate loosening of the legacy validation.
func (s *source) listPaths() []string {
	if len(s.includeObjects) > 0 {
		return s.includeObjects
	}
	return s.cfg.GetPaths()
}

// listTables resolves listPaths() into concrete YtNodeMeta entries. It opens
// the snapshot TX on first use (ensureTx), lists and Snapshot-locks every
// table exactly once per snapshot and caches the result. Locking fills in
// NodeID, which later unlocks the columnFilter-based schema projection.
func (s *source) listTables(ctx context.Context) (cypressmeta.YtNodes, error) {
	if err := s.ensureTx(ctx); err != nil {
		return nil, xerrors.Errorf("unable to ensure snapshot tx: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nodes != nil {
		return s.nodes, nil
	}
	tbls, err := cypressmeta.ListNodes(ctx, s.tx, s.cfg.GetCluster(), s.listPaths(), []yt.NodeType{yt.NodeTable}, false /* skipLinkFollowing */, s.logger)
	if err != nil {
		return nil, xerrors.Errorf("error listing tables: %w", err)
	}
	// Parity with the legacy uniformParts guard: every abstract2 snapshot
	// failed on more than 1024 tables regardless of sharding.
	if err := dataobjects.CheckTableCountLimit(tbls); err != nil {
		return nil, xerrors.Errorf("table count check failed: %w", err)
	}
	if s.columnFilter == nil {
		columnFilter, err := buildColumnFilter(s.yt, s.includeObjects)
		if err != nil {
			return nil, xerrors.Errorf("failed to build column filter: %w", err)
		}
		s.columnFilter = columnFilter
	}
	for _, tbl := range tbls {
		lock, lockErr := s.tx.LockNode(ctx, tbl.OriginalYPath(), yt.LockSnapshot, nil)
		if lockErr != nil {
			return nil, xerrors.Errorf("unable to lock table %s: %w", tbl.OriginalYPath(), lockErr)
		}
		nodeID := lock.NodeID
		tbl.NodeID = &nodeID
	}
	s.nodes = tbls
	return s.nodes, nil
}

// resolveTable returns the YtNodeMeta whose relative path matches the given TableID.
func (s *source) resolveTable(ctx context.Context, tid abstract.TableID) (*cypressmeta.YtNodeMeta, error) {
	tables, err := s.listTables(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		if tableIDFor(t) == tid {
			return t, nil
		}
	}
	return nil, xerrors.Errorf("table %s not found in configured paths", tid.Fqtn())
}

// tableDescriptionToPart materializes a *dataobjects.Part for LoadTable.
//
// When Filter is a YSON PartKey (secondary workers see this after
// SetShardingContext / ShardTable), the row range is parsed straight out of
// it. When Filter is empty (main worker preflight paths or non-sharded
// loads), we resolve the table and construct a full-range part.
func (s *source) tableDescriptionToPart(ctx context.Context, table abstract.TableDescription) (*dataobjects.Part, error) {
	s.mu.Lock()
	txID := s.txID
	s.mu.Unlock()
	if len(table.Filter) > 0 {
		key, err := dataobjects.ParsePartKey(string(table.Filter))
		if err != nil {
			return nil, xerrors.Errorf("cannot parse part key %q: %w", string(table.Filter), err)
		}
		return dataobjects.NewPart(key.Table, key.NodeID, key.Range(), txID), nil
	}
	node, err := s.resolveTable(ctx, abstract.TableID{Namespace: table.Schema, Name: table.Name})
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve table for load: %w", err)
	}
	if node.NodeID == nil {
		return nil, xerrors.Errorf("table %s is not locked; LoadTable requires an active BeginSnapshot", table.Name)
	}
	full := ypath.Interval(ypath.RowIndex(0), ypath.RowIndex(node.RowCount))
	return dataobjects.NewPart(node.Name, *node.NodeID, full, txID), nil
}
