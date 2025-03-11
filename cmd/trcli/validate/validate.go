package validate

import (
	"github.com/spf13/cobra"
	"github.com/transferria/transferria/cmd/trcli/config"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
)

func ValidateCommand() *cobra.Command {
	var transferParams string
	validationCommand := &cobra.Command{
		Use:   "validate",
		Short: "Validate a transfer configuration",
		RunE:  validate(&transferParams),
	}
	validationCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	return validationCommand
}

func validate(transferYaml *string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}

		if err := transfer.Src.Validate(); err != nil {
			return xerrors.Errorf("source validation failed: %w", err)
		}

		logger.Log.Infof("%s 👌source config", transfer.Src.GetProviderType())

		if err := transfer.Dst.Validate(); err != nil {
			return xerrors.Errorf("target validation failed: %w", err)
		}

		logger.Log.Infof("%s 👌destination config", transfer.Dst.GetProviderType())

		if err := transfer.Validate(); err != nil {
			return xerrors.Errorf("transfer validation failed: %w", err)
		}

		logger.Log.Infof("%s 👌transfer config", transfer.Type)
		return nil
	}
}
