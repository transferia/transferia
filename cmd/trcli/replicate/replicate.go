package replicate

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/transferria/transferria/cmd/trcli/activate"
	"github.com/transferria/transferria/cmd/trcli/config"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/dataplane/provideradapter"
	"github.com/transferria/transferria/pkg/runtime/local"
)

func ReplicateCommand(cp *coordinator.Coordinator, rt abstract.Runtime, registry metrics.Registry) *cobra.Command {
	var transferParams string
	var metricsPrefix string

	replicationCommand := &cobra.Command{
		Use:   "replicate",
		Short: "Start local replication",
		RunE:  replicate(cp, rt, &transferParams, registry, metricsPrefix),
	}
	replicationCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	replicationCommand.Flags().StringVar(&metricsPrefix, "metrics-prefix", "", "Optional prefix por Prometheus metrics")
	return replicationCommand
}

func replicate(cp *coordinator.Coordinator, rt abstract.Runtime, transferYaml *string, registry metrics.Registry, metricsPrefix string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}
		transfer.Runtime = rt

		if metricsPrefix != "" {
			registry = registry.WithPrefix(metricsPrefix)
		}

		return RunReplication(*cp, transfer, registry)
	}
}

func RunReplication(cp coordinator.Coordinator, transfer *model.Transfer, registry metrics.Registry) error {
	if err := provideradapter.ApplyForTransfer(transfer); err != nil {
		return xerrors.Errorf("unable to adapt transfer: %w", err)
	}
	st, err := cp.GetTransferState(transfer.ID)
	if err != nil {
		return xerrors.Errorf("unable to get transfer state: %w", err)
	}
	if stt, ok := st["status"]; !ok || stt.Generic == nil {
		if err := activate.RunActivate(cp, transfer, registry, 0); err != nil {
			return xerrors.Errorf("unable to activate transfer: %w", err)
		}
		if err := cp.SetTransferState(transfer.ID, map[string]*coordinator.TransferStateData{
			"status": {
				Generic:             "activated",
				IncrementalTables:   nil,
				OraclePosition:      nil,
				MysqlGtid:           nil,
				MysqlBinlogPosition: nil,
				YtStaticPart:        nil,
			},
		}); err != nil {
			return xerrors.Errorf("unable to set transfer state: %w", err)
		}
	}

	for {
		worker := local.NewLocalWorker(
			cp,
			transfer,
			registry.WithTags(map[string]string{
				"resource_id": transfer.ID,
				"name":        transfer.TransferName,
			}),
			logger.Log,
		)
		err := worker.Run()
		if abstract.IsFatal(err) {
			if err := (cp).RemoveTransferState(transfer.ID, []string{"status"}); err != nil {
				return xerrors.Errorf("unable to cleanup status state: %w", err)
			}
			return err
		}
		if err := worker.Stop(); err != nil {
			logger.Log.Warnf("unable to stop worker: %v", err)
		}
		logger.Log.Warnf("worker failed: %v, restart", err)
		time.Sleep(10 * time.Second)
	}
}
