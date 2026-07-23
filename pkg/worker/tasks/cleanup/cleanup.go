package cleanup

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

var cleanupKinds = map[model.CleanupType]abstract.Kind{
	model.Drop:     abstract.DropTableKind,
	model.Truncate: abstract.TruncateTableKind,
}

func CleanupTables(sink abstract.AsyncSink, tables abstract.TableMap, cleanupType model.CleanupType) error {
	cleanupStart := time.Now()
	logger.Log.Debugf("CleanupTables START tables=%d cleanupType=%s", len(tables), string(cleanupType))
	var toDelete abstract.TableMap
	var nextToDelete abstract.TableMap
	var errByTable map[string]error

	if cleanupType == model.DisabledCleanup {
		logger.Log.Info("Cleanup is disabled, nothing to do")
		return nil
	}

	kind, ok := cleanupKinds[cleanupType]
	if !ok {
		return xerrors.Errorf("unsupported cleanup type: %v", cleanupType)
	}

	prevToDelete := 0
	toDelete = tables
	i := 0
	var changeItems []abstract.ChangeItem
	for tID := range toDelete {
		logger.Log.Infof("bulk cleanup (%v): try to %v %v", string(cleanupType), string(cleanupType), tID.Name)
		ci := new(abstract.ChangeItem)
		ci.Kind = kind
		ci.Schema = tID.Namespace
		ci.Table = tID.Name
		ci.CommitTime = uint64(time.Now().UnixNano())
		ci.TableSchema = tables[tID].Schema
		changeItems = append(changeItems, *ci)
	}
	logger.Log.Debugf("CleanupTables: bulk AsyncPush START items=%d elapsed=%v", len(changeItems), time.Since(cleanupStart))
	if err := <-sink.AsyncPush(changeItems); err != nil {
		logger.Log.Warn(fmt.Sprintf("bulk cleanup (%v) failed, try via iterators", string(cleanupType)), log.Error(err))
		logger.Log.Debugf("CleanupTables: bulk AsyncPush FAILED err=%v elapsed=%v", err, time.Since(cleanupStart))
	} else {
		logger.Log.Infof("bulk cleanup (%v) done", string(cleanupType))
		logger.Log.Debugf("CleanupTables: bulk AsyncPush DONE elapsed=%v", time.Since(cleanupStart))
		logger.Log.Debugf("CleanupTables ALL DONE (bulk) elapsed=%v", time.Since(cleanupStart))
		return nil
	}
	for {
		if len(toDelete) == 0 || len(toDelete) == prevToDelete {
			break
		}

		i += 1
		logger.Log.Infof("start %v iteration to cleanup (%v) tables", i, string(cleanupType))
		logger.Log.Debugf("CleanupTables: iteration %d START remaining=%d elapsed=%v", i, len(toDelete), time.Since(cleanupStart))
		errByTable = map[string]error{}
		prevToDelete = len(toDelete)
		nextToDelete = abstract.TableMap{}
		for tID, tInfo := range toDelete {
			logger.Log.Infof("iteration %v: try to %v %v", i, string(cleanupType), tID.Name)
			logger.Log.Debugf("CleanupTables: per-table AsyncPush START table=%s elapsed=%v", tID.Name, time.Since(cleanupStart))
			if err := <-sink.AsyncPush([]abstract.ChangeItem{
				{Kind: kind, Schema: tID.Namespace, Table: tID.Name, CommitTime: uint64(time.Now().UnixNano())},
			}); err != nil {
				logger.Log.Warn(fmt.Sprintf("%v failed, try on next iteration", string(cleanupType)), log.Any("table", tID.Name), log.Error(err))
				logger.Log.Debugf("CleanupTables: per-table AsyncPush FAILED table=%s err=%v elapsed=%v", tID.Name, err, time.Since(cleanupStart))
				errByTable[tID.Name] = xerrors.Errorf("failed to cleanup table %s: %w", tID.Name, err)
				nextToDelete[tID] = tInfo
			} else {
				logger.Log.Debugf("CleanupTables: per-table AsyncPush DONE table=%s elapsed=%v", tID.Name, time.Since(cleanupStart))
			}
		}
		toDelete = nextToDelete
	}
	errsFlat := make([]error, 0, len(errByTable))
	for _, err := range errByTable {
		errsFlat = append(errsFlat, err)
	}
	if len(toDelete) > 0 {
		logger.Log.Debugf("CleanupTables FAILED remaining=%d elapsed=%v", len(toDelete), time.Since(cleanupStart))
		return fmt.Errorf("failed to cleanup tables: %w", multierr.Combine(errsFlat...))
	}
	logger.Log.Debugf("CleanupTables ALL DONE elapsed=%v", time.Since(cleanupStart))
	return nil
}
