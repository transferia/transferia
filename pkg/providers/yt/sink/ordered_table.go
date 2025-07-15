// Description of ordered_table.go: https://st.yandex-team.ru/TM-887#5fbcddfd5372c4026b073bab
package sink

import (
	"context"
	"encoding/json"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/ptr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/kv"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/go/ytlock"
)

type OrderedTable struct {
	ytClient               yt.Client
	path                   ypath.Path
	logger                 log.Logger
	metrics                *stats.SinkerStats
	schema                 []abstract.ColSchema
	config                 yt2.YtDestinationModel
	mutex                  sync.Mutex
	partitionToTabletIndex *kv.YtDynTableKVWrapper
	lbOffsetToRowIndex     *kv.YtDynTableKVWrapper
	pathToReshardLock      ypath.Path
	tabletsCount           uint32
	knownPartitions        map[string]bool
}

// structs for dyn tables

type PartitionToTabletIndexKey struct {
	Partition string `yson:"partition,key"`
}

type PartitionToTabletIndexVal struct {
	TabletIndex  uint32 `yson:"tablet_index"`
	LastLbOffset uint64 `yson:"last_lb_offset"`
}

type LbOffsetToRowIndexKey struct {
	TabletIndex uint32 `yson:"tablet_index,key"`
	LbOffset    uint64 `yson:"lb_offset,key"`
}

type LbOffsetToRowIndexVal struct {
	MinRowIndex uint64 `yson:"min_row_index"`
	MaxRowIndex uint64 `yson:"max_row_index"`
}

//nolint:descriptiveerrors
func (t *OrderedTable) getMaxLbOffsetMaxRowIndexForTabletIndex(ctx context.Context, tx yt.TabletTx, partition string) (bool, uint64, uint64, error) {
	found, output, err := t.partitionToTabletIndex.GetValueByKeyTx(ctx, tx, PartitionToTabletIndexKey{Partition: partition})
	if err != nil {
		return false, 0, 0, err
	}
	if !found {
		return false, 0, 0, xerrors.Errorf("partition %v not found in partitionToTabletIndex", partition)
	}
	val := output.(*PartitionToTabletIndexVal)

	if val.LastLbOffset == 0 {
		return false, 0, 0, nil
	}

	found, minMaxRowIndex, err := t.lbOffsetToRowIndex.GetValueByKeyTx(ctx, tx, LbOffsetToRowIndexKey{TabletIndex: val.TabletIndex, LbOffset: val.LastLbOffset})
	if err != nil {
		return false, 0, 0, err
	}
	if !found {
		return false, 0, 0, xerrors.Errorf("TabletIndex: %v & LbOffset: %v not found in lbOffsetToRowIndex", val.TabletIndex, val.LastLbOffset)
	}
	rowIndex := minMaxRowIndex.(*LbOffsetToRowIndexVal)

	return true, val.LastLbOffset, rowIndex.MaxRowIndex, nil
}

//

func (t *OrderedTable) fillTabletCount(ctx context.Context) error {
	var tabletCount uint32
	err := t.ytClient.GetNode(ctx, t.path.Attr("tablet_count"), &tabletCount, nil)
	if err != nil {
		return err
	}
	t.tabletsCount = tabletCount
	return nil
}

//nolint:descriptiveerrors
func (t *OrderedTable) Init() error {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	exist, err := t.ytClient.NodeExists(ctx, t.path, nil)
	if err != nil {
		return err
	}
	if exist {
		err := t.fillTabletCount(ctx)
		if err != nil {
			return err
		}
	}

	s := make([]schema.Column, len(t.schema))
	for i, col := range t.schema {
		s[i] = schema.Column{
			Name: col.ColumnName,
			Type: fixDatetime(&col),
		}
	}

	systemAttrs := map[string]interface{}{
		"primary_medium":     t.config.PrimaryMedium(),
		"tablet_cell_bundle": t.config.CellBundle(),
		"optimize_for":       t.config.OptimizeFor(),
	}

	if t.config.TTL() > 0 {
		systemAttrs["min_data_versions"] = 0
		systemAttrs["max_data_versions"] = 1
		systemAttrs["max_data_ttl"] = t.config.TTL()
	}

	ddlCommand := map[ypath.Path]migrate.Table{}
	ddlCommand[t.path] = migrate.Table{
		Schema: schema.Schema{
			UniqueKeys: false,
			Columns:    s,
		},
		Attributes: t.config.MergeAttributes(systemAttrs),
	}

	return backoff.Retry(func() error {
		if err := migrate.EnsureTables(ctx, t.ytClient, ddlCommand, onConflictTryAlterWithoutNarrowing(ctx, t.ytClient)); err != nil {
			t.logger.Error("Init table error", log.Error(err))
			return err
		}
		if !exist {
			if err := t.ensureTablets(t.config.InitialTabletCount()); err != nil {
				return err
			}
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 10))
}

func getTabletIndexByPartition(partition abstract.Partition) (uint32, error) {
	var dcNum uint32
	switch partition.Cluster {
	case "sas":
		dcNum = 0
	case "vla":
		dcNum = 1
	case "man":
		dcNum = 2
	case "iva":
		dcNum = 3
	case "myt":
		dcNum = 4
	default: // for partition "default"
		return 0, nil
	}

	return partition.Partition*5 + dcNum, nil
}

//nolint:descriptiveerrors
func (t *OrderedTable) getTabletIndexByPartitionPersistent(partition string) (uint32, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var partitionStruct abstract.Partition
	if err := json.NewDecoder(strings.NewReader(partition)).Decode(&partitionStruct); err != nil {
		return 0, xerrors.Errorf("failed to parse partition: %w", err)
	}

	currTablet, err := getTabletIndexByPartition(partitionStruct)
	if err != nil {
		return 0, err
	}

	if t.knownPartitions[partition] {
		return currTablet, nil
	}

	found, output, err := t.partitionToTabletIndex.GetValueByKey(ctx, PartitionToTabletIndexKey{Partition: partition})
	if err != nil {
		return 0, err
	}
	if found {
		val := output.(*PartitionToTabletIndexVal)
		if val.TabletIndex != currTablet {
			return 0, xerrors.Errorf("val.TabletIndex != currTablet. val.TabletIndex: %v, currTablet: %v", val.TabletIndex, currTablet)
		}
		t.knownPartitions[partition] = true
	} else {
		t.logger.Infof("Insert to __partition_to_tablet_index pair - partition: %v, tablet_index: %v", partition, currTablet)
		err = t.partitionToTabletIndex.InsertRow(ctx, PartitionToTabletIndexKey{Partition: partition}, PartitionToTabletIndexVal{TabletIndex: currTablet, LastLbOffset: 0})
		if err != nil {
			return 0, err
		}
	}

	return currTablet, nil
}

func validateInput(input []abstract.ChangeItem) error {
	partition := input[0].Part()
	prevOffset, found := input[0].Offset()
	if !found {
		return xerrors.Errorf("validateInput - got changeItem without offset: %v", input[0].ToJSONString())
	}
	for _, el := range input {
		if el.Part() != partition {
			return xerrors.Errorf("validateInput - got input for >1 partition: %v & %v", el.Part(), partition)
		}
		currOffset, found := el.Offset()
		if !found {
			return xerrors.Errorf("validateInput - got changeItem without offset: %v, partition: %v", el.ToJSONString(), partition)
		}
		if currOffset < prevOffset {
			return xerrors.Errorf("offsets are not in non-decreasing order. currOffset: %v, prevOffset: %v, partition: %v", currOffset, prevOffset, partition)
		}
		prevOffset = currOffset
	}
	return nil
}

func getMinMaxLbOffset(input []abstract.ChangeItem) (minLfOffset uint64, maxLbOffset uint64) {
	minLfOffset, _ = input[0].Offset()
	maxLbOffset, _ = input[len(input)-1].Offset()
	return minLfOffset, maxLbOffset
}

//nolint:descriptiveerrors
func (t *OrderedTable) Write(input []abstract.ChangeItem) error {
	t.logger.Infof("#change_items in Write: %v", len(input))
	if len(input) == 0 {
		return nil
	}
	if err := validateInput(input); err != nil {
		return err
	}

	// now input - for sure:
	//     - !empty
	//     - contains changeItems only for the same partition
	//     - every changeItem has an offset
	//     - offset goes in non-decreasing order

	partition := input[0].Part()
	tabletIndex, err := t.getTabletIndexByPartitionPersistent(partition)
	if err != nil {
		return err
	}
	if err := t.ensureTablets(tabletIndex); err != nil {
		return err
	}

	minLbOffset, maxLbOffset := getMinMaxLbOffset(input)
	t.logger.Infof("schedule upload tablet #%v, partition: %v, len(input): %v, minLbOffset: %v, maxLbOffset: %v\n", tabletIndex, partition, len(input), minLbOffset, maxLbOffset)

	err = backoff.Retry(func() error {
		if err := t.insertToSpecificTablet(tabletIndex, input); err != nil {
			t.logger.Error("unable to insert in tablet #"+strconv.Itoa(int(tabletIndex)), log.Error(err))
			return err
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 10))

	return err
}

type InsertChangeItem struct {
	DiscardBigValues bool
	TabletIndex      uint32
	ChangeItem       abstract.ChangeItem
}

func (i *InsertChangeItem) MarshalYSON(w *yson.Writer) error {
	w.BeginMap()
	for idx, colName := range i.ChangeItem.ColumnNames {
		w.MapKeyString(colName)
		value, err := RestoreWithLengthLimitCheck(i.ChangeItem.TableSchema.Columns()[idx], i.ChangeItem.ColumnValues[idx], i.DiscardBigValues, YtDynMaxStringLength)
		if err != nil {
			return xerrors.Errorf("Unable to restore value for column '%s': %w", colName, err)
		}
		w.Any(value)
	}
	w.MapKeyString("$tablet_index")
	w.Any(i.TabletIndex)
	w.EndMap()
	return w.Err()
}

var reRowConflict = regexp.MustCompile(`.*row lock conflict due to concurrent write.*`)

//nolint:descriptiveerrors
func (t *OrderedTable) insertToSpecificTablet(tabletIndex uint32, changeItems []abstract.ChangeItem) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.config.WriteTimeoutSec())*time.Second) // start tx
	defer cancel()

	tx, rollbacks, err := beginTabletTransaction(ctx, t.ytClient, true, t.logger)
	if err != nil {
		return xerrors.Errorf("Unable to beginTabletTransaction: %w", err)
	}
	defer rollbacks.Do()

	partition := changeItems[0].Part()
	foundMaxCommittedOffset, maxCommittedLbOffset, prevMaxRowIndex, err := t.getMaxLbOffsetMaxRowIndexForTabletIndex(ctx, tx, partition)
	if err != nil {
		return err
	}

	insertChangeItems := make([]interface{}, 0)

	skippedCount := 0
	for _, changeItem := range changeItems {
		changeOffset, found := changeItem.Offset()
		if !found {
			return xerrors.Errorf("changeItem doesn't contain '_offset' column: " + changeItem.ToJSONString()) // TODO - change it when TM-1290
		}
		if (maxCommittedLbOffset != 0) && (changeOffset <= maxCommittedLbOffset) {
			skippedCount++
			continue
		}

		insertChangeItems = append(insertChangeItems, &InsertChangeItem{TabletIndex: tabletIndex, ChangeItem: changeItem, DiscardBigValues: t.config.DiscardBigValues()})
	}

	if skippedCount != 0 {
		t.metrics.Table(path.Base(t.path.String()), "skip", skippedCount)
	}

	lastChangeItem := changeItems[len(changeItems)-1]
	lastOffset, found := lastChangeItem.Offset()
	if !found {
		return xerrors.Errorf("changeItem doesn't contain '_offset' column: " + lastChangeItem.ToJSONString()) // TODO - change it when TM-1290
	}
	if len(insertChangeItems) == 0 {
		t.logger.Warnf("tablet %v deduplicated (maxCommittedLbOffset:%v lastOffset:%v)", tabletIndex, maxCommittedLbOffset, lastOffset)
		return nil
	}

	if err := tx.InsertRows(ctx, t.path, insertChangeItems, nil); err != nil { // insert to main table
		return err
	}

	var minRowIndex, maxRowIndex uint64
	if !foundMaxCommittedOffset { // if it's first record for this tablet
		minRowIndex = 0
		maxRowIndex = uint64(len(insertChangeItems) - 1)
	} else {
		minRowIndex = prevMaxRowIndex + 1
		maxRowIndex = minRowIndex + uint64(len(insertChangeItems)-1)
	}

	err = t.lbOffsetToRowIndex.InsertRowTx( // insert into '__lb_offset_to_row_index' metainfo dyn table
		ctx,
		tx,
		LbOffsetToRowIndexKey{TabletIndex: tabletIndex, LbOffset: lastOffset},
		LbOffsetToRowIndexVal{MinRowIndex: minRowIndex, MaxRowIndex: maxRowIndex},
	)
	if err != nil {
		return err
	}

	err = t.partitionToTabletIndex.InsertRowTx( // insert into '__partition_to_tablet_index' metainfo dyn table
		ctx,
		tx,
		PartitionToTabletIndexKey{Partition: partition},
		PartitionToTabletIndexVal{TabletIndex: tabletIndex, LastLbOffset: lastOffset},
	)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		if yterrors.ContainsMessageRE(err, reRowConflict) {
			t.metrics.Table(path.Base(t.path.String()), "row_lock_conflict", skippedCount)
			t.logger.Error("Row lock conflict - that's ok. That means another worker got the same partition after hard re-balancing", log.Error(err))
		}
		return err
	}
	rollbacks.Cancel()
	return nil
}

//nolint:descriptiveerrors
func (t *OrderedTable) ensureTablets(maxTablet uint32) error {
	newTabletCount := maxTablet + 1

	if t.tabletsCount >= newTabletCount {
		return nil
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err := t.fillTabletCount(ctx)
	if err != nil {
		return err
	}
	if t.tabletsCount >= newTabletCount { // for the case, when table resharded in another thread when we waited lock
		return nil
	}

	lock := ytlock.NewLock(t.ytClient, t.pathToReshardLock)
	_, err = lock.Acquire(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release(ctx) }()

	err = t.fillTabletCount(ctx)
	if err != nil {
		return err
	}
	if t.tabletsCount >= newTabletCount { // for the case, when table resharded in another job when we waited lock
		return nil
	}

	t.logger.Infof("Reshard table, newTabletCount: %v, prev tabletsCount: %v", newTabletCount, t.tabletsCount)

	if err := yt2.MountUnmountWrapper(ctx, t.ytClient, t.path, migrate.UnmountAndWait); err != nil {
		return err
	}

	t.logger.Infof("table unmounted")

	if err := t.ytClient.ReshardTable(ctx, t.path, &yt.ReshardTableOptions{
		TabletCount: ptr.Int(int(newTabletCount)),
	}); err != nil {
		return err
	}

	t.logger.Infof("table resharded")

	if err := yt2.MountUnmountWrapper(ctx, t.ytClient, t.path, migrate.MountAndWait); err != nil {
		//nolint:descriptiveerrors
		return err
	}

	t.logger.Infof("table mounted")

	err = t.fillTabletCount(ctx)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}

	return nil
}

func NewOrderedTable(ytClient yt.Client, path ypath.Path, schema []abstract.ColSchema, cfg yt2.YtDestinationModel, metrics *stats.SinkerStats, logger log.Logger) (GenericTable, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dir, tableName, err := ypath.Split(path)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}
	tableName = tableName[1:]

	partitionToTabletIndex, err := kv.NewYtDynTableKVWrapper(
		ctx,
		ytClient,
		yt2.SafeChild(dir, "meta", tableName+"__partition_to_tablet_index"),
		*new(PartitionToTabletIndexKey),
		*new(PartitionToTabletIndexVal),
		cfg.CellBundle(),
		map[string]interface{}{
			"primary_medium":     cfg.PrimaryMedium(),
			"tablet_cell_bundle": cfg.CellBundle(),
		},
	)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}

	lbOffsetToRowIndex, err := kv.NewYtDynTableKVWrapper(
		ctx,
		ytClient,
		yt2.SafeChild(dir, "meta", tableName+"__lb_offset_to_row_index"),
		*new(LbOffsetToRowIndexKey),
		*new(LbOffsetToRowIndexVal),
		cfg.CellBundle(),
		map[string]interface{}{
			"primary_medium":     cfg.PrimaryMedium(),
			"tablet_cell_bundle": cfg.CellBundle(),
			"min_data_versions":  0,
			"max_data_versions":  1,
			"max_data_ttl":       3 * 86400 * 1000,
		},
	)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}

	t := OrderedTable{
		ytClient:               ytClient,
		path:                   path,
		logger:                 logger,
		metrics:                metrics,
		schema:                 schema,
		config:                 cfg,
		mutex:                  sync.Mutex{},
		partitionToTabletIndex: partitionToTabletIndex,
		lbOffsetToRowIndex:     lbOffsetToRowIndex,
		pathToReshardLock:      yt2.SafeChild(dir, "meta", tableName+"__reshard_lock"),
		tabletsCount:           1,
		knownPartitions:        map[string]bool{},
	}

	if err := t.Init(); err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}

	return &t, nil
}
