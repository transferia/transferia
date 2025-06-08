package local

import (
	"context"
	"runtime/debug"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/dataplane/provideradapter"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/metering"
	"github.com/transferia/transferia/pkg/runtime/shared"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type Spec struct {
	ID   string
	Src  model.Source
	Dst  model.Destination
	Type abstract.TransferType

	Transformation *model.Transformation
	DataObjects    *model.DataObjects
	FolderID       string

	TypeSystemVersion int
}

const ReplicationStatusMessagesCategory string = "replication"

const healthReportPeriod time.Duration = 1 * time.Minute
const replicationRetryInterval time.Duration = 10 * time.Second

func RunReplicationWithMeteringTags(ctx context.Context, cp coordinator.Coordinator, transfer *model.Transfer, registry metrics.Registry, runtimeTags map[string]interface{}) error {
	metering.InitializeWithTags(transfer, nil, runtimeTags)
	shared.ApplyRuntimeLimits(transfer.Runtime)
	return runReplication(ctx, cp, transfer, registry, logger.Log)
}

func RunReplication(ctx context.Context, cp coordinator.Coordinator, transfer *model.Transfer, registry metrics.Registry) error {
	metering.Initialize(transfer, nil)
	shared.ApplyRuntimeLimits(transfer.Runtime)
	return runReplication(ctx, cp, transfer, registry, logger.Log)
}

func runReplication(ctx context.Context, cp coordinator.Coordinator, transfer *model.Transfer, registry metrics.Registry, lgr log.Logger) error {
	if err := provideradapter.ApplyForTransfer(transfer); err != nil {
		return xerrors.Errorf("unable to adapt transfer: %w", err)
	}

	var previousAttemptErr error = nil
	retryCount := int64(1)

	replicationStats := stats.NewReplicationStats(registry)

	for {
		replicationStats.StartUnix.Set(float64(time.Now().Unix()))

		attemptErr, attemptAgain := replicationAttempt(ctx, cp, transfer, registry, lgr, replicationStats, retryCount)
		if !attemptAgain {
			return xerrors.Errorf("replication failed: %w", attemptErr)
		}

		if !errors.EqualCauses(previousAttemptErr, attemptErr) {
			status := errors.ToTransferStatusMessage(attemptErr)
			status.Type = coordinator.WarningStatusMessageType
			if err := cp.OpenStatusMessage(transfer.ID, ReplicationStatusMessagesCategory, status); err != nil {
				logger.Log.Warn("failed to set status message to report an error in replication", log.Error(err), log.NamedError("replication_error", err))
			}
		}

		logger.Log.Warn("replication failed and will be retried without dataplane restart after a given timeout", log.Error(attemptErr), log.Duration("timeout", replicationRetryInterval))

		time.Sleep(replicationRetryInterval)
		retryCount += 1
		previousAttemptErr = attemptErr
	}
}

func replicationAttempt(ctx context.Context, cp coordinator.Coordinator, transfer *model.Transfer, registry metrics.Registry, lgr log.Logger, replicationStats *stats.ReplicationStats, retryCount int64) (err error, attemptAgain bool) {
	replicationStats.Running.Set(float64(1))
	defer func() {
		replicationStats.Running.Set(float64(0))
	}()

	var attemptErr error = nil

	healthReportTicker := time.NewTicker(healthReportPeriod)
	defer healthReportTicker.Stop()

	replicationErrCh := make(chan error)
	iterCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func(errCh chan<- error) {
		errCh <- iteration(iterCtx, cp, transfer, registry, lgr)
		close(errCh)
	}(replicationErrCh)
waitingForReplicationErr:
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-healthReportTicker.C:
			if err := cp.CloseStatusMessagesForCategory(transfer.ID, ReplicationStatusMessagesCategory); err != nil {
				logger.Log.Warn("failed to close status messages", log.Error(err))
			}
			reportTransferHealth(ctx, cp, transfer.ID, retryCount, nil)
		case err := <-replicationErrCh:
			if err == nil {
				err = xerrors.New("replication terminated without an error. This is an anomaly, see logs for error details")
			}
			attemptErr = err
			break waitingForReplicationErr
		}
	}

	reportTransferHealth(ctx, cp, transfer.ID, retryCount, attemptErr)

	if abstract.IsFatal(attemptErr) {
		err := metering.Agent().Stop() // if already stopped, nothing will happen
		if err != nil {
			logger.Log.Warnf("could not stop metering: %v", err)
		}
		ensureReplicationFailure(cp, transfer.ID, attemptErr)
		// status message will be set to error by the error processing code, so the status message is only set below for non-fatal errors
		return xerrors.Errorf("a fatal error occurred in replication: %w", attemptErr), false
	}
	// This 300 IQ hack is an attempt to mitigate __possible__ connection leak in postgresql connector
	// The idea is to restart the whole process and drop all existing connections
	// Possibly the leak itself is already fixed and the hack may be removed but nobody knows
	if strings.Contains(attemptErr.Error(), "SQLSTATE 53300") {
		logger.Log.Error("replication failed, will restart the whole dataplane", log.Error(attemptErr))
		return xerrors.Errorf("replication failed, dataplane must be restarted: %w", attemptErr), false
	}

	return attemptErr, true
}

func reportTransferHealth(ctx context.Context, cp coordinator.Coordinator, transferID string, retryCount int64, reportErr error) {
	lastErrorText := ""
	if reportErr != nil {
		lastErrorText = reportErr.Error()
	}
	if err := cp.TransferHealth(
		ctx,
		transferID,
		&coordinator.TransferHeartbeat{
			RetryCount: int(retryCount),
			LastError:  lastErrorText,
		},
	); err != nil {
		logger.Log.Warn("unable to report transfer health", log.Error(err), log.NamedError("last_error", reportErr))
	}
}

// Does not return unless an error occurs
func iteration(ctx context.Context, cp coordinator.Coordinator, dataFlow *model.Transfer, registry metrics.Registry, lgr log.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = xerrors.Errorf("Panic: %v", r)
			logger.Log.Error("Job panic", log.Error(err), log.String("stacktrace", string(debug.Stack())))
		}
	}()
	worker := NewLocalWorker(cp, dataFlow, registry, lgr)

	defer func() {
		if err := worker.Stop(); err != nil {
			logger.Log.Warn("unable to stop worker", log.Error(err))
		}
	}() //nolint
	workerErr := make(chan error)
	go func() {
		workerErr <- worker.Run()
	}()
	select {
	case err := <-workerErr:
		return err
	case <-ctx.Done():
		return nil
	}
}

func ensureReplicationFailure(cp coordinator.Coordinator, transferID string, err error) {
	logger.Log.Error("Fatal replication error", log.Error(err))
	for {
		// since replication is terminated we need to close old replication issues
		if err := cp.CloseStatusMessagesForCategory(transferID, ReplicationStatusMessagesCategory); err != nil {
			logger.Log.Warn("failed to close status messages", log.Error(err))
			time.Sleep(5 * time.Second)
			continue
		}
		if cErr := cp.FailReplication(transferID, err); cErr != nil {
			logger.Log.Error("Cannot fail replication", log.Error(cErr))
			time.Sleep(5 * time.Second)
			continue
		}
		return
	}
}
