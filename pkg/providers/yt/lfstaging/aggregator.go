//go:build !disable_yt_provider

package lfstaging

import (
	"context"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytlock"
	"golang.org/x/xerrors"
)

type tableAggregator struct {
	config      *sinkConfig
	ytClient    yt.Client
	logger      log.Logger
	writers     map[string]*stagingWriter
	periodTimer *time.Timer
}

func newTableAggregator(
	config *sinkConfig,
	ytClient yt.Client,
	logger log.Logger,
) *tableAggregator {
	return &tableAggregator{
		config:      config,
		ytClient:    ytClient,
		logger:      logger,
		writers:     map[string]*stagingWriter{},
		periodTimer: nil,
	}
}

func (a *tableAggregator) Start() {
	a.logger.Info("LfStaging - Starting aggregator fiber")
	for {
		lock := ytlock.NewLock(a.ytClient, a.config.tmpPath.Child("__lock"))
		_, err := lock.Acquire(context.Background())
		if err != nil {
			a.logger.Info("LfStaging - unable to acquire lock", log.Error(err))
			time.Sleep(10 * time.Minute)
			continue
		}

		a.periodTimer = time.NewTimer(a.config.aggregationPeriod)
		startedAggregatingAt := time.Now()
		if err := a.aggregateTables(); err != nil {
			a.logger.Warn("LfStaging - aggregateTables returned error", log.Error(err))
		}

		err = lock.Release(context.Background())
		if err != nil {
			a.logger.Warn("LfStaging - unable to release lock", log.Error(err))
		}

		timeSpentAggregating := time.Since(startedAggregatingAt)
		if timeSpentAggregating > a.config.aggregationPeriod {
			a.logger.Warnf(
				"LfStaging - took %v s aggregating with period %v s. You should probably tune the config",
				int64(timeSpentAggregating/time.Second),
				int64(a.config.aggregationPeriod/time.Second),
			)
		} else {
			time.Sleep(a.config.aggregationPeriod - timeSpentAggregating)
		}
	}
}

func (a *tableAggregator) writerForTopic(tx yt.Tx, topic string, now time.Time) (*stagingWriter, error) {
	writer, ok := a.writers[topic]
	if !ok {
		// TODO(ionagamed): remove this when all topics are set for all transfers
		a.config.topic = topic
		writer, err := newStagingWriter(tx, a.config, now)
		if err != nil {
			return nil, err
		}
		a.writers[topic] = writer
		return writer, nil
	} else {
		return writer, nil
	}
}

func (a *tableAggregator) aggregateTablesImplNew(tx yt.Tx, tmpNodes []ytNode, now time.Time) error {
	err := closeGaps(tx, a.config, now)
	if err != nil {
		return err
	}

	mergeSpec := spec.Merge()
	atLeastOneDataTable := false

	for _, node := range tmpNodes {
		if node.Type == "table" {
			if a.config.usePersistentIntermediateTables && !node.IsWriterFinished {
				a.logger.Infof("Not adding %v - persistent intermediate tables are enabled and this table is locked", node.Path)
				continue
			}
			atLeastOneDataTable = true
			a.logger.Infof("Adding merge input %v", node.Path)
			mergeSpec.AddInput(ypath.Path(node.Path))
		}
	}

	if !atLeastOneDataTable {
		a.logger.Info("No data tables to merge")
		return nil
	}

	topicDir := a.config.stagingPath.Child(a.config.topic)
	outputTableName := makeStagingTableName(a.config.aggregationPeriod, now)
	outputTablePath := topicDir.Child(outputTableName)
	mergeSpec.OutputTablePath = outputTablePath
	mergeSpec.Pool = a.config.ytPool

	createTopicDirOptions := &yt.CreateNodeOptions{
		Recursive:      true,
		IgnoreExisting: true,
	}
	if _, err := tx.CreateNode(context.TODO(), topicDir, yt.NodeMap, createTopicDirOptions); err != nil {
		return xerrors.Errorf("cannot create output topic dir: %w", err)
	}

	if _, err := tx.CreateNode(context.TODO(), outputTablePath, yt.NodeTable, nil); err != nil {
		return xerrors.Errorf("cannot create output table: %w", err)
	}

	a.logger.Infof("Starting merge with ytPool='%v'", a.config.ytPool)

	opID, err := tx.StartOperation(
		context.TODO(),
		yt.OperationMerge,
		mergeSpec,
		nil,
	)
	if err != nil {
		return xerrors.Errorf("cannot start merge job: %w", err)
	}

	var opStatus *yt.OperationStatus
	statusRequestDelay := time.Second * 10

	for opStatus == nil || !opStatus.State.IsFinished() {
		opStatus, err = a.ytClient.GetOperation(context.TODO(), opID, nil)
		if err != nil {
			return xerrors.Errorf("cannot request merge job status: %w", err)
		}

		if !opStatus.State.IsFinished() {
			a.logger.Infof("Merge operation %v is not finished - trying again in %v seconds", opID, statusRequestDelay/time.Second)
			time.Sleep(statusRequestDelay)
		}
	}

	switch opStatus.State {
	case yt.StateAborted:
		return xerrors.Errorf("merge operation %v was aborted", opID)
	case yt.StateFailed:
		errorMessage := "<no error message>"
		if opStatus.Result != nil && opStatus.Result.Error != nil {
			errorMessage = opStatus.Result.Error.Message
		}
		return xerrors.Errorf("merge operation %v has failed: %v", opID, errorMessage)
	default:
		a.logger.Infof("Merge operation %v has finished successfully", opID)
	}

	a.logger.Infof("Starting metadata merging")

	metadata := newLogbrokerMetadata()
	for _, node := range tmpNodes {
		if node.Type == "table" {
			if a.config.usePersistentIntermediateTables {
				if !node.IsWriterFinished {
					a.logger.Infof("Skipping %v for cleanup because of a writer lock", node.Path)
					continue
				}
			}

			nodeMetadata, err := lbMetaFromTableAttr(tx, ypath.Path(node.Path))
			if err != nil {
				return xerrors.Errorf("cannot get metadata for node %v: %w", node.Path, err)
			}

			if err := metadata.Merge(nodeMetadata); err != nil {
				return xerrors.Errorf("cannot merge metdata: %w", err)
			}

			if err := a.cleanUpTmpTable(tx, node); err != nil {
				return xerrors.Errorf("cannot clean up tmp table: %w", err)
			}
		}
	}
	if err := metadata.saveIntoTableAttr(tx, outputTablePath); err != nil {
		return xerrors.Errorf("cannot save metadata into output table: %w", err)
	}

	return nil
}

func (a *tableAggregator) aggregateTablesImplOld(tx yt.Tx, tmpNodes []ytNode, now time.Time) error {
	continueReading := true
	for _, node := range tmpNodes {
		if !continueReading {
			break
		}
		if node.Type != "table" {
			continue
		}
		if err := a.processTableRows(tx, node, now); err != nil {
			a.rollbackWriters()
			return xerrors.Errorf("cannot process table rows for table '%v': %w", node.Path, err)
		}
		if err := a.cleanUpTmpTable(tx, node); err != nil {
			a.rollbackWriters()
			return xerrors.Errorf("cannot clean up tmp table '%v': %w", node.Path, err)
		}

		// reading all of the pending tables might take a long time
		// committing changes every aggregationPeriod helps with that
		select {
		case <-a.periodTimer.C:
			a.logger.Warn("LfStaging - Aggregation period passed, but not all tables have been processed")
			continueReading = false
		default:
		}
	}

	a.logger.Infof("LfStaging - Committing staging table writer")

	if err := a.commitWriters(tx); err != nil {
		a.rollbackWriters()
		return xerrors.Errorf("cannot commit staging writer: %w", err)
	}

	return nil
}

func (a *tableAggregator) aggregateTables() error {
	a.logger.Info("LfStaging - Starting to aggregate tables")

	return yt.ExecTx(context.Background(), a.ytClient, func(_ context.Context, tx yt.Tx) error {
		now := time.Now()

		tmpNodes, err := listNodes(tx, a.config.tmpPath)
		if err != nil {
			return xerrors.Errorf("cannot list tmp nodes: %w", err)
		}

		a.logger.Infof("LfStaging - Aggregating %v tables", len(tmpNodes))

		if a.config.useNewMetadataFlow {
			err := a.aggregateTablesImplNew(tx, tmpNodes, now)
			if err != nil {
				return xerrors.Errorf("cannot aggregate tables with new metadata flow: %w", err)
			}
		} else {
			err := a.aggregateTablesImplOld(tx, tmpNodes, now)
			if err != nil {
				return xerrors.Errorf("cannot aggregate tables with old metadata flow: %w", err)
			}
		}

		return nil
	}, &yt.ExecTxOptions{
		RetryOptions: &yt.ExecTxRetryOptionsNone{},
	})
}

func (a *tableAggregator) processTableRows(tx yt.Tx, node ytNode, now time.Time) error {
	reader, err := tx.ReadTable(context.Background(), a.config.tmpPath.Child(node.Name), nil)
	if err != nil {
		return xerrors.Errorf("cannot create table reader: %w", err)
	}
	defer reader.Close()

	intermediateRowsCount := 0

	for reader.Next() {
		var row intermediateRow

		if err := reader.Scan(&row); err != nil {
			return xerrors.Errorf("cannot scan reader: %w", err)
		}

		err := a.processRow(tx, row, now)
		if err != nil {
			return xerrors.Errorf("failed converting the row: %w", err)
		}

		intermediateRowsCount++
	}

	a.logger.Infof("LfStaging - Moved %v rows", intermediateRowsCount)

	if reader.Err() != nil {
		return xerrors.Errorf("failed reading the table: %w", reader.Err())
	}

	return nil
}

func (a *tableAggregator) processRow(tx yt.Tx, row intermediateRow, now time.Time) error {
	writer, err := a.writerForTopic(tx, row.TopicName, now)
	if err != nil {
		return xerrors.Errorf("cannot get a staging writer for topic '%v': %w", row.TopicName, err)
	}

	err = writer.Write(row)
	if err != nil {
		return xerrors.Errorf("cannot write row into the writer: %w", err)
	}

	return nil
}

func (a *tableAggregator) cleanUpTmpTable(tx yt.Tx, node ytNode) error {
	if err := tx.RemoveNode(context.Background(), a.config.tmpPath.Child(node.Name), nil); err != nil {
		return xerrors.Errorf("failed removing node: %w", err)
	} else {
		return nil
	}
}

func (a *tableAggregator) rollbackWriters() {
	for _, writer := range a.writers {
		writer.Rollback()
	}
	a.writers = map[string]*stagingWriter{}
}

func (a *tableAggregator) commitWriters(tx yt.Tx) error {
	for _, writer := range a.writers {
		if err := writer.Commit(tx); err != nil {
			return err
		}
	}
	a.writers = map[string]*stagingWriter{}
	return nil
}
