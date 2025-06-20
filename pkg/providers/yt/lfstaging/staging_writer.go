//go:build !disable_yt_provider

package lfstaging

import (
	"context"
	"fmt"
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/xerrors"
)

type stagingWriter struct {
	writer    yt.TableWriter
	config    *sinkConfig
	tablePath ypath.Path
	tx        yt.Tx

	now        time.Time
	roundedNow time.Time

	metadata *logbrokerMetadata
}

func roundTimestampToNearest(ts time.Time, interval time.Duration) time.Time {
	seconds := ts.Unix()
	intervalSeconds := int64(interval / time.Second)
	roundedSeconds := seconds / intervalSeconds * intervalSeconds

	return time.Unix(roundedSeconds, 0)
}

func makeStagingTableName(period time.Duration, now time.Time) string {
	roundedTS := roundTimestampToNearest(now, period)
	return fmt.Sprintf("%v-300", roundedTS.Unix())
}

func newStagingWriter(
	tx yt.Tx,
	config *sinkConfig,
	now time.Time,
) (*stagingWriter, error) {
	tableDir := config.stagingPath.Child(config.topic)
	newTableAttrs := map[string]interface{}{}

	if config.ytAccount != "" {
		newTableAttrs["account"] = config.ytAccount
	}

	_, err := tx.CreateNode(context.Background(), tableDir, yt.NodeMap, &yt.CreateNodeOptions{
		Recursive:      true,
		IgnoreExisting: true,
		Attributes:     newTableAttrs,
	})
	if err != nil {
		return nil, xerrors.Errorf("Cannot create staging topic dir: %w", err)
	}

	tablePath := tableDir.Child(makeStagingTableName(config.aggregationPeriod, now))

	exists, err := tx.NodeExists(context.Background(), tablePath, nil)
	if err != nil {
		return nil, xerrors.Errorf("LfStaging - Failed to check existence of staging output table: %w", err)
	}

	if exists {
		return nil, xerrors.Errorf("LfStaging - Staging table with path %v already exists", tablePath)
	}

	schema, err := lfStagingRowSchema()
	if err != nil {
		return nil, xerrors.Errorf("Cannot infer staging row schema: %w", err)
	}

	_, err = yt.CreateTable(
		context.Background(),
		tx,
		tablePath,
		yt.WithSchema(schema),
		yt.WithAttributes(newTableAttrs),
	)
	if err != nil {
		return nil, xerrors.Errorf("Cannot create topic table: %w", err)
	}

	metadata := newLogbrokerMetadata()
	metadata.topic = config.topic

	return &stagingWriter{
		writer:     nil,
		config:     config,
		tablePath:  tablePath,
		tx:         tx,
		now:        now,
		roundedNow: roundTimestampToNearest(now, config.aggregationPeriod),
		metadata:   metadata,
	}, nil
}

func (sw *stagingWriter) Write(row intermediateRow) error {
	outputRow := lfStagingRowFromIntermediate(row)

	if sw.writer == nil {
		writer, err := sw.tx.WriteTable(context.Background(), sw.tablePath, nil)
		if err != nil {
			return xerrors.Errorf("Cannot create staging area writer: %w", err)
		}
		sw.writer = writer
	}

	if err := sw.writer.Write(outputRow); err != nil {
		return xerrors.Errorf("Cannot write into the writer: %w", err)
	}

	sw.metadata.AddIntermediateRow(row)

	return nil
}

func (sw *stagingWriter) Rollback() {
	_ = sw.writer.Rollback()
}

func (sw *stagingWriter) Commit(tx yt.Tx) error {
	err := closeGaps(tx, sw.config, sw.now)
	if err != nil {
		return xerrors.Errorf("Cannot close table gaps: %w", err)
	}

	return sw.CommitWithoutClosingGaps(tx)
}

func (sw *stagingWriter) CommitWithoutClosingGaps(tx yt.Tx) error {
	if sw.writer != nil {
		if err := sw.writer.Commit(); err != nil {
			return xerrors.Errorf("Cannot commit raw writer: %w", err)
		}
	}

	err := storeYtState(tx, sw.config.tmpPath, ytState{
		LastTableTS: sw.roundedNow.Unix(),
	})
	if err != nil {
		return xerrors.Errorf("Cannot set the table state: %w", err)
	}

	err = sw.metadata.saveIntoTableAttr(tx, sw.tablePath)
	if err != nil {
		return xerrors.Errorf("Cannot set attributes: %w", err)
	}

	return nil
}
