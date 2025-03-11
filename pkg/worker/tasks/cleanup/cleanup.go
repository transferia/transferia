package cleanup

import (
	"fmt"
	"time"

	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

var cleanupKinds = map[model.CleanupType]abstract.Kind{
	model.Drop:     abstract.DropTableKind,
	model.Truncate: abstract.TruncateTableKind,
}

func CleanupTables(sink abstract.AsyncSink, tables abstract.TableMap, cleanupType model.CleanupType) error {
	var toDelete abstract.TableMap
	var nextToDelete abstract.TableMap
	var errors map[string]string

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
	if err := <-sink.AsyncPush(changeItems); err != nil {
		logger.Log.Warn(fmt.Sprintf("bulk cleanup (%v) failed, try via iterators", string(cleanupType)), log.Error(err))
	} else {
		logger.Log.Infof("bulk cleanup (%v) done", string(cleanupType))
		return nil
	}
	for {
		if len(toDelete) == 0 || len(toDelete) == prevToDelete {
			break
		}

		i += 1
		logger.Log.Infof("start %v iteration to cleanup (%v) tables", i, string(cleanupType))
		errors = map[string]string{}
		prevToDelete = len(toDelete)
		nextToDelete = abstract.TableMap{}
		for tID, tInfo := range toDelete {
			logger.Log.Infof("iteration %v: try to %v %v", i, string(cleanupType), tID.Name)
			if err := <-sink.AsyncPush([]abstract.ChangeItem{
				{Kind: kind, Schema: tID.Namespace, Table: tID.Name, CommitTime: uint64(time.Now().UnixNano())},
			}); err != nil {
				logger.Log.Warn(fmt.Sprintf("%v failed, try on next iteration", string(cleanupType)), log.Any("table", tID.Name), log.Error(err))
				errors[tID.Name] = err.Error()
				nextToDelete[tID] = tInfo
			}
		}
		toDelete = nextToDelete
	}
	if len(toDelete) > 0 {
		return fmt.Errorf("%v", errors)
	}
	return nil
}
