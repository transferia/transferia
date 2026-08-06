package coordinator_utils

import (
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"go.ytsaurus.tech/library/go/core/log"
)

func calculateTransferStateSize(in map[string]*coordinator.TransferStateData) (int, map[string]int, error) {
	sumSize := 0
	result := make(map[string]int)
	for k, v := range in {
		sumSize += len(k) + 1
		switch vStr := v.Generic.(type) {
		case string:
			result[k] = len(vStr)
			sumSize += len(vStr) + 1
		default:
			return 0, nil, fmt.Errorf("calculateTransferStateSize - unknown type: %T", vStr)
		}
	}
	return sumSize, result, nil
}

func logTransferStateSizeOrFail(logger log.Logger, in map[string]*coordinator.TransferStateData) error {
	resultSize, resultMap, err := calculateTransferStateSize(in)
	if err != nil {
		return xerrors.Errorf("unable to calculateTransferStateSize, err: %w", err)
	}
	if resultSize < 1024*1024 {
		logger.Info(fmt.Sprintf("transferState size: %d", resultSize), log.Any("perPartitionSize", resultMap))
	} else { // if resultSize < 3*1024*1024 -- TODO - uncomment it after check - check if it exceeds on 'dtti21kvk7djvpfgtllh' or not
		logger.Warn(fmt.Sprintf("transferState size: %d", resultSize), log.Any("perPartitionSize", resultMap))
	}
	// else {
	// 	return abstract.NewFatalError(xerrors.Errorf("transferState for one worker exceeded 3MB: %d", resultSize))
	// }
	// TODO - uncomment it after check - check if it exceeds on 'dtti21kvk7djvpfgtllh' or not -- TM-10383
	return nil
}
