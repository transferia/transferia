package check

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/transferria/transferria/cmd/trcli/config"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/worker/tasks"
)

func CheckCommand() *cobra.Command {
	var transferParams string
	checkCommand := &cobra.Command{
		Use:   "check",
		Short: "Check transfer locally",
		Args:  cobra.MatchAll(cobra.ExactArgs(0)),
		RunE:  check(&transferParams),
	}
	checkCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	return checkCommand
}

func check(transferYaml *string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}
		return RunCheck(transfer)
	}
}

func testTransfer(transfer model.Transfer) error {
	res := tasks.TestEndpoint(context.Background(), &tasks.TestEndpointParams{
		Transfer:             &transfer,
		TransformationConfig: nil,
		ParamsSrc: &tasks.EndpointParam{
			Type:  transfer.SrcType(),
			Param: transfer.SrcJSON(),
		},
		ParamsDst: &tasks.EndpointParam{
			Type:  transfer.DstType(),
			Param: transfer.DstJSON(),
		},
	}, abstract.NewTestResult())

	logger.Log.Infof("done %v checks", len(res.Checks))
	for t, v := range res.Checks {
		if !v.Success {
			if v.Error != nil {
				logger.Log.Errorf("	check: %s: 💀 failed: \n%+v", t, v.Error)
			} else {
				logger.Log.Errorf("	check: %s: 🙉 skip", t)
			}
		} else {
			logger.Log.Infof("	check: %s: 👌", t)
		}
	}
	if len(res.Preview) > 0 {
		logger.Log.Infof("found %v tables", len(res.Preview))
		for k, rows := range res.Preview {
			logger.Log.Infof("	table: %s", k.Fqtn())
			for _, c := range res.Schema[k].Schema.Columns() {
				keyMark := ""
				if c.PrimaryKey {
					keyMark = "🔑"
				}
				logger.Log.Infof("		column: %s %s %s (%s)", keyMark, c.ColumnName, c.OriginalType, c.DataType)
			}
			abstract.Dump(rows)
		}
	} else {
		logger.Log.Warn("no tables found")
	}

	return nil
}

func RunCheck(transfer *model.Transfer) error {
	if err := testTransfer(*transfer); err != nil {
		return xerrors.Errorf("failed testing target: %w", err)
	}

	return nil
}
