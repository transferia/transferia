package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/randutil"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var trueConst = true
var _ GenericTable = new(SingleStaticTable)

// SingleStaticTable is a specialization of StaticTable but for single table
// which behaves like GenericTable for dynamic sinker.
// Assumed that only one goroutine works with SingleStaticTable at moment,
// otherwise all methods should be protected with primitives in future
type SingleStaticTable struct {
	ytClient        yt.Client
	tableName       string
	tmpTableName    string
	mergedTableName string
	dirPath         ypath.Path
	logger          log.Logger
	tx              yt.Tx
	tableWriter     yt.TableWriter
	schema          []abstract.ColSchema
	config          yt2.YtDestinationModel
	metrics         *stats.SinkerStats
	transferID      string
	cleanupType     model.CleanupType
	pathToBinary    ypath.Path

	writtenCount uint
	skippedCount uint
}

func (t *SingleStaticTable) schemaKeys() []string {
	var ret = make([]string, 0)
	for _, col := range t.schema {
		if col.PrimaryKey {
			ret = append(ret, col.ColumnName)
		}
	}
	return ret
}

func (t *SingleStaticTable) getMergeTableScheme() schema.Schema {
	columns := abstract.ToYtSchema(t.schema, true)
	for i := range columns {
		columns[i].Expression = ""
	}
	mergeTableScheme := schema.Schema{
		Columns: columns,
		Strict:  &trueConst,
	}
	if len(mergeTableScheme.KeyColumns()) == len(mergeTableScheme.Columns) {
		// key amount equals column amount, so we should add at least one data column in order to produce table
		mergeTableScheme = mergeTableScheme.Append(schema.Column{Name: "__dummy", Type: schema.TypeAny, Required: false})
	}
	return mergeTableScheme
}

// Init starts transaction and creates temporary table for writing
//
//nolint:descriptiveerrors
func (t *SingleStaticTable) init() error {
	ctx := context.Background()
	var rollbacks util.Rollbacks
	defer rollbacks.Do()

	// create parent Cypres node outsize of transaction
	dirCreateOptions := yt.CreateNodeOptions{
		Recursive:      true,
		IgnoreExisting: true,
	}
	_, err := t.ytClient.CreateNode(ctx, t.dirPath, yt.NodeMap, &dirCreateOptions)
	if err != nil {
		return err
	}
	tx, err := t.ytClient.BeginTx(ctx, nil)
	if err != nil {
		t.logger.Error("cannot begin internal transaction for table", log.Any("table", t.tableName), log.Error(err))
		return err
	}
	rollbacks.Add(func() {
		err := tx.Abort()
		if err != nil {
			t.logger.Error("abort transaction error", log.Error(err))
		}
	})
	t.logger.Info("started transaction for table", log.Any("table", t.tableName), log.Error(err), log.Any("transaction", tx.ID()))
	t.tx = tx

	ytSchema := abstract.ToYtSchema(t.schema, true)
	// remove key columns for the first table
	for i := range ytSchema {
		ytSchema[i].SortOrder = ""
		ytSchema[i].Expression = ""
	}
	scheme := schema.Schema{
		Columns: ytSchema,
		Strict:  &trueConst, // do not use t.config.Strict -- it is irrelevant to yt 'Strict'
	}
	createOptions := yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"schema": scheme,
		},
		TransactionOptions: &yt.TransactionOptions{},
		Recursive:          true,
		IgnoreExisting:     false,
	}
	tmpTablePath := yt2.SafeChild(t.dirPath, t.tmpTableName)
	logger.Log.Info(
		"Creating YT table with options",
		log.String("tmpPath", tmpTablePath.String()),
		log.Any("options", createOptions),
	)

	if _, err := tx.CreateNode(ctx, tmpTablePath, yt.NodeTable, &createOptions); err != nil {
		return err
	}
	opts := yt.WriteTableOptions{TableWriter: t.config.Spec().GetConfig()}
	w, err := tx.WriteTable(ctx, tmpTablePath, &opts)
	if err != nil {
		return err
	}
	t.logger.Info("initialized new single static writer", log.String("dirPath", t.dirPath.String()),
		log.String("table", t.tableName), log.String("tmpTable", t.tmpTableName), log.Any("transaction", tx.ID()))
	t.tableWriter = w

	rollbacks.Cancel()
	return nil
}

// Write writes change items to table
// Note: after calling this method object is still usable, but if
// error returned: object is NOT usable
func (t *SingleStaticTable) Write(input []abstract.ChangeItem) error {
	var rollbacks util.Rollbacks
	defer rollbacks.Do()
	rollbacks.Add(func() {
		err := t.tx.Abort()
		if err != nil {
			t.logger.Error("cannot abort transaction", log.Error(err))
		}
	})
	err := t.write(input)
	if err != nil {
		return xerrors.Errorf("cannot write: %w", err)
	}
	rollbacks.Cancel()
	return nil
}

func (t *SingleStaticTable) write(input []abstract.ChangeItem) error {
	colNameToIndex := map[string]int{}
	for i, c := range t.schema {
		colNameToIndex[c.ColumnName] = i
	}

	for _, item := range input {
		switch item.Kind {
		case abstract.InsertKind:
			row := map[string]interface{}{}
			for idx, col := range item.ColumnNames {
				schemeID := colNameToIndex[col]
				var err error
				row[col], err = RestoreWithLengthLimitCheck(item.TableSchema.Columns()[schemeID], item.ColumnValues[idx], t.config.DiscardBigValues(), YtDynMaxStringLength)
				if err != nil {
					return xerrors.Errorf("cannot restore value for column '%s': %w", col, err)
				}
			}
			if err := t.tableWriter.Write(row); err != nil {
				s, _ := json.MarshalIndent(item, "", "    ")
				logger.Log.Error("cannot write changeItem to static table", log.String("table", t.tmpTableName),
					log.Error(err), log.String("item", string(s)))
				return err
			}
			t.writtenCount++
		default:
			s, _ := json.MarshalIndent(item, "", "    ")
			logger.Log.Warn("wrong change item kind in static table write stream -- only inserts expected, change item skipped", log.String("table", t.tmpTableName),
				log.Any("kind", item.Kind), log.String("item", string(s)))
			t.skippedCount++
		}
	}
	return nil
}

// Commit tries to commit the transaction and performs some sorting and moving operations
// After commit object is not usable
func (t *SingleStaticTable) Commit(ctx context.Context) error {
	defer func() {
		err := t.tx.Abort()
		if err != nil {
			t.logger.Error("cannot abort transaction", log.Error(err))
		}
	}()
	return t.commit(ctx)
}

func (t *SingleStaticTable) Abort() error {
	return t.tx.Abort()
}

func (t *SingleStaticTable) commit(ctx context.Context) error {
	tableYPath := yt2.SafeChild(t.dirPath, t.tableName)
	tmpTableYPath := yt2.SafeChild(t.dirPath, t.tmpTableName)
	mergedTableYPath := yt2.SafeChild(t.dirPath, t.mergedTableName)

	// step 1: commit writes to table
	t.logger.Info("commit static table [step 1] -- commit writes to table", log.String("table", t.tmpTableName))
	t.logger.Info("try commit", log.String("table", t.tmpTableName), log.Any("transaction", t.tx.ID()),
		log.Any("path", tmpTableYPath))

	if err := t.tableWriter.Commit(); err != nil {
		t.logger.Error("cannot commit table writer, aborting transaction",
			log.String("table", t.tmpTableName),
			log.Any("transaction", t.tx.ID()))
		//nolint:descriptiveerrors
		return err
	}

	if len(t.schemaKeys()) == 0 {
		if !t.config.Ordered() {
			return abstract.NewFatalError(NoKeyColumnsFound)
		}
		// table without keys is already done for making it ordered and dynamic
		moveOptions := yt2.ResolveMoveOptions(t.ytClient, tmpTableYPath, false)
		if _, err := t.tx.MoveNode(ctx, tmpTableYPath, mergedTableYPath, moveOptions); err != nil {
			t.logger.Error("cannot move tmp table, aborting transaction", log.String("table", t.mergedTableName),
				log.Any("transaction", t.tx.ID()), log.Any("path", tmpTableYPath))
			//nolint:descriptiveerrors
			return err
		}
	} else {

		// step 2: create merge table with appropriate schema
		t.logger.Info("commit static table [step 2] -- create merge table", log.String("table", t.mergedTableName))
		mergeTableScheme := t.getMergeTableScheme()
		atomicity := string(yt.AtomicityNone)
		if t.config.Atomicity() != "" {
			atomicity = string(t.config.Atomicity())
		}
		createMergeTableOptions := yt.CreateNodeOptions{
			Attributes: map[string]interface{}{
				"schema":       mergeTableScheme,
				"optimize_for": t.config.OptimizeFor(),
				"atomicity":    atomicity,
			},
			TransactionOptions: &yt.TransactionOptions{},
			Recursive:          true,
			IgnoreExisting:     false,
		}
		logger.Log.Info(
			"Creating YT merge table with options",
			log.String("tmpPath", mergedTableYPath.String()),
			log.Any("options", createMergeTableOptions),
		)
		if _, err := t.tx.CreateNode(ctx, mergedTableYPath, yt.NodeTable, &createMergeTableOptions); err != nil {
			//nolint:descriptiveerrors
			return err
		}

		// step 3: launch merge of (yet single) table into correct table
		t.logger.Info("commit static table [step 3] -- sort table", log.String("src_table", t.tmpTableName),
			log.String("dst_table", t.tmpTableName))
		sortOperationID, err := t.tx.StartOperation(ctx, yt.OperationSort, map[string]interface{}{
			"pool":                  t.config.Pool(),
			"input_table_paths":     []ypath.Path{tmpTableYPath},
			"output_table_path":     mergedTableYPath,
			"sort_by":               t.schemaKeys(),
			"schema_inference_mode": "from_output",
			"partition_job_io":      map[string]interface{}{"table_writer": map[string]interface{}{"block_size": 256 * (2 << 10)}},
			"merge_job_io":          map[string]interface{}{"table_writer": map[string]interface{}{"block_size": 256 * (2 << 10)}},
			"sort_job_io":           map[string]interface{}{"table_writer": map[string]interface{}{"block_size": 256 * (2 << 10)}},
			"max_failed_job_count":  5,
		}, nil)
		if err != nil {
			//nolint:descriptiveerrors
			return err
		}
		for {
			status, err := t.ytClient.GetOperation(ctx, sortOperationID, nil)
			if err != nil {
				//nolint:descriptiveerrors
				return err
			}
			if status.State == yt.StateFailed {
				//nolint:descriptiveerrors
				return status.Result.Error
			}
			t.logger.Info("Job status: sorting of snapshot static table", log.String("status", string(status.State)))
			if status.State.IsFinished() {
				break
			}
			timeoutSeconds := 2
			t.logger.Info(fmt.Sprintf("Wait %d seconds for another poll of job status: sorting of snapshot static table", timeoutSeconds))
			time.Sleep(time.Duration(timeoutSeconds) * time.Second)
		}

		// step 4: move table to new location
		// remove previous node as well
		if err = t.tx.RemoveNode(ctx, tmpTableYPath, nil); err != nil {
			//nolint:descriptiveerrors
			return err
		}
	}

	t.logger.Info("commit static table [step +oo] -- commit", log.String("table", t.tableName))
	if err := t.tx.Commit(); err != nil {
		t.logger.Error("cannot commit transaction, aborting...", log.String("table", t.tableName),
			log.Any("transaction", t.tx.ID()))
		//nolint:descriptiveerrors
		return err
	}

	t.metrics.Table(string(tableYPath), "rows", int(t.writtenCount))
	t.metrics.Table(string(tableYPath), "skip", int(t.skippedCount))
	return nil
}

func finishSingleStaticTableLoading(
	ctx context.Context,
	client yt.Client,
	logger log.Logger,
	dirPath ypath.Path,
	transferID string,
	tableName string,
	cleanupType model.CleanupType,
	pathToBinary ypath.Path,
	tableWriterSpec interface{},
	buildAttrs func(schema schema.Schema) map[string]interface{},
	tableRotationEnabled bool,
) error {
	partInfix := buildPartInfix(transferID)
	tablePrefix := tableName + partInfix
	tmpSuffix := buildTmpSuffix(transferID)
	var cleanupPath ypath.Path
	if cleanupType != model.DisabledCleanup {
		cleanupPath = dirPath.Child(tableName)
	}
	return Merge(ctx, client, logger, dirPath, cleanupPath, tablePrefix, partInfix, tmpSuffix, pathToBinary, tableWriterSpec, buildAttrs, tableRotationEnabled)
}

func (t *SingleStaticTable) finishLoading() error {
	var tableWriterSpec interface{}
	if spec := t.config.Spec(); spec != nil {
		tableWriterSpec = spec.GetConfig()
	}
	return finishSingleStaticTableLoading(context.TODO(), t.ytClient, t.logger, t.dirPath, t.transferID, t.tableName, t.cleanupType, t.pathToBinary, tableWriterSpec,
		func(schema schema.Schema) map[string]interface{} {
			return buildDynamicAttrs(getCols(schema), t.config)
		}, t.config.Rotation() != nil)
}

func (t *SingleStaticTable) UpdateSchema(schema []abstract.ColSchema) {
	t.schema = schema
}

func NewSingleStaticTable(
	ytClient yt.Client,
	dirPath ypath.Path,
	tableName string,
	schema []abstract.ColSchema,
	cfg yt2.YtDestinationModel,
	jobIndex int,
	transferID string,
	cleanupType model.CleanupType,
	metrics *stats.SinkerStats,
	logger log.Logger,
	pathToBinary ypath.Path,
) (*SingleStaticTable, error) {
	if cfg == nil {
		return nil, fmt.Errorf("parameter 'cfg *server.YtDestination' should not be 'nil'")
	}

	partInfix := buildPartInfix(transferID)
	partSuffix := fmt.Sprintf("_%v_%v", jobIndex, randutil.GenerateAlphanumericString(8))
	sstName := tableName + partInfix + partSuffix
	qst := SingleStaticTable{
		ytClient:        ytClient,
		tableName:       tableName,
		tmpTableName:    sstName + "_tmp",
		mergedTableName: sstName,
		dirPath:         dirPath,
		logger:          logger,
		tx:              nil,
		tableWriter:     nil,
		schema:          schema,
		config:          cfg,
		metrics:         metrics,
		writtenCount:    0,
		skippedCount:    0,
		transferID:      transferID,
		cleanupType:     cleanupType,
		pathToBinary:    pathToBinary,
	}

	if err := qst.init(); err != nil {
		return nil, err
	}
	return &qst, nil
}

func buildPartInfix(transferID string) string {
	return fmt.Sprintf("_%v_sst_part", transferID)
}

func buildTmpSuffix(transferID string) string {
	return fmt.Sprintf("_%v_sst_tmp", transferID)
}

func CleanupSingleStaticTable(ctx context.Context, client yt.Client, logger log.Logger, dirPath ypath.Path, transferID string) error {
	dirExists, err := client.NodeExists(ctx, dirPath, nil)
	if err != nil {
		return xerrors.Errorf("unable to check if target directory '%v' exists: %w", dirPath, err)
	}
	if !dirExists {
		logger.Infof("target directory '%v' does not exist", dirPath)
		return nil
	}

	var nodes []struct {
		Name string `yson:",value"`
	}
	err = client.ListNode(ctx, dirPath, &nodes, &yt.ListNodeOptions{})
	if err != nil {
		return xerrors.Errorf("unable to list nodes: %w", err)
	}

	partInfix := buildPartInfix(transferID)
	tmpSuffix := buildTmpSuffix(transferID)
	logger.Infof("going to recursively cleanup nodes with infix %q or suffix %q in directory %q", partInfix, tmpSuffix, dirPath)
	for _, node := range nodes {
		if !strings.Contains(node.Name, partInfix) && !strings.HasSuffix(node.Name, tmpSuffix) {
			continue
		}

		path := dirPath.Child(node.Name)
		options := &yt.RemoveNodeOptions{
			Recursive: true,
		}
		err = client.RemoveNode(ctx, path, options)
		if err != nil {
			return xerrors.Errorf("unable to remove node '%v': %w", path, err)
		}
		logger.Infof("successfully removed node: %v", path)
	}

	return nil
}
