package tasks

import (
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
)

//---------------------------------------------------------------------------------------------------------------------
// This file is needed for lb-in-the-middle cases
//
// 'terminal' here - is NOT(lb) endpoint
// terminal_transfer - transfer where at least one endpoint is terminal
//
// We support here either terminal->terminal or one-lb-in-the-middle cases
// If there are more than 1 lb in chain - we are not supporting this.
// It's not fundamental restriction, we can support long chains, but we need good reasons for that!
//
// terminal transfers:
//     - terminal->lb       - left terminal type
//     - lb->terminal       - right terminal type
//     - terminal->terminal
//---------------------------------------------------------------------------------------------------------------------

func GetLeftTerminalTransfers(cp coordinator.Coordinator, transfer model.Transfer) ([]*model.Transfer, error) {
	if isLeftTerminalType(transfer) {
		return []*model.Transfer{&transfer}, nil
	} else {
		return getLeftTerminalTransfersForLbSrc(cp, transfer)
	}
}

func GetLeftTerminalSrcEndpoints(cp coordinator.Coordinator, transfer model.Transfer) ([]model.Source, error) {
	transfers, err := GetLeftTerminalTransfers(cp, transfer)
	if err != nil {
		return nil, err
	}
	result := make([]model.Source, 0)
	for _, currTransfer := range transfers {
		result = append(result, currTransfer.Src)
	}
	return result, nil
}

func GetRightTerminalTransfers(cp coordinator.Coordinator, transfer model.Transfer) ([]*model.Transfer, error) {
	if isRightTerminalType(transfer) {
		return []*model.Transfer{&transfer}, nil
	} else {
		return getRightTerminalTransfersForLbDst(cp, transfer)
	}
}

func GetRightTerminalDstEndpoints(cp coordinator.Coordinator, transfer model.Transfer) ([]model.Destination, error) {
	transfers, err := GetRightTerminalTransfers(cp, transfer)
	if err != nil {
		return nil, err
	}
	result := make([]model.Destination, 0)
	for _, currTransfer := range transfers {
		result = append(result, currTransfer.Dst)
	}
	return result, nil
}

func isLeftTerminalType(transfer model.Transfer) bool {
	switch transfer.Src.(type) {
	case model.TransitionalEndpoint:
		return false
	default:
		return true
	}
}

func isRightTerminalType(transfer model.Transfer) bool {
	switch transfer.Dst.(type) {
	case model.TransitionalEndpoint:
		return false
	default:
		return true
	}
}

func getLeftTerminalTransfersForLbSrc(cp coordinator.Coordinator, transfer model.Transfer) ([]*model.Transfer, error) {
	result := make([]*model.Transfer, 0)

	transfers, err := cp.GetTransfers([]model.TransferStatus{model.Running}, transfer.ID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get transfers: %w", err)
	}

	src, ok := transfer.Src.(model.TransitionalEndpoint)

	if !ok {
		return nil, xerrors.Errorf("transfer %v is not lb-src", transfer)
	}

	for _, currTransfer := range transfers {
		if dst, ok := currTransfer.Dst.(model.TransitionalEndpoint); ok {
			if dst.TransitionalWith(src) {
				logger.Log.Infof("Found lb origin %v", currTransfer.ID)
				if isLeftTerminalType(*currTransfer) {
					result = append(result, currTransfer)
				}
			}
		}
	}

	return result, nil
}

func getRightTerminalTransfersForLbDst(cp coordinator.Coordinator, transfer model.Transfer) ([]*model.Transfer, error) {
	result := make([]*model.Transfer, 0)

	transfers, err := cp.GetTransfers([]model.TransferStatus{model.Running}, transfer.ID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get transfers: %w", err)
	}

	src, ok := transfer.Dst.(model.TransitionalEndpoint)

	if !ok {
		return nil, xerrors.Errorf("transfer %v is not lb-dst", transfer)
	}

	for _, currTransfer := range transfers {
		if dst, ok := currTransfer.Src.(model.TransitionalEndpoint); ok {
			if dst.TransitionalWith(src) {
				logger.Log.Infof("Found lb target %v", currTransfer.ID)
				if isRightTerminalType(*currTransfer) {
					result = append(result, currTransfer)
				}
			}
		}
	}

	return result, nil
}
