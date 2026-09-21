package transferhelpers

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	_ "github.com/transferia/transferia/pkg/dataplane"
	"github.com/transferia/transferia/pkg/dataplane/provideradapter"
)

var TransferID = "dtt"

func GenerateTransferID(testName string) string {
	return fmt.Sprintf("%s_%s", TransferID, strings.ToLower(testName))
}

func InitSrcDst(transferID string, src model.Source, dst model.Destination, transferType abstract.TransferType) {
	src.WithDefaults()
	dst.WithDefaults()

	transfer := &model.Transfer{
		ID:   transferID,
		Type: transferType,
		Src:  src,
		Dst:  dst,
	}
	// fill dependent fields on drugs
	_ = provideradapter.ApplyForTransfer(transfer)
	transfer.FillDependentFields()
}

func MakeTransfer(transferID string, src model.Source, dst model.Destination, transferType abstract.TransferType) *model.Transfer {
	src.WithDefaults()
	dst.WithDefaults()
	transfer := &model.Transfer{
		ID:   transferID,
		Type: transferType,
		Src:  src,
		Dst:  dst,
		Runtime: &abstract.LocalRuntime{
			Host:       "localhost",
			CurrentJob: 0,
			ShardingUpload: abstract.ShardUploadParams{
				JobCount:     1,
				ProcessCount: 1,
			},
		},
	}
	transfer.FillDependentFields()
	// fill dependent fields on drugs
	_ = provideradapter.ApplyForTransfer(transfer)

	return transfer
}

func WithLocalRuntime(transfer *model.Transfer, jobCount int, processCount int) *model.Transfer {
	transfer.Runtime = &abstract.LocalRuntime{
		Host:       "",
		CurrentJob: 0,
		ShardingUpload: abstract.ShardUploadParams{
			JobCount:     jobCount,
			ProcessCount: processCount,
		},
	}
	return transfer
}

func MakeTransferForIncrementalSnapshot(transferID string, src model.Source, dst model.Destination, transferType abstract.TransferType,
	namespace, tableName, cursorField, initialState string, incrementDelay int64) *model.Transfer {

	regularSnapshot := &abstract.RegularSnapshot{
		Incremental: []abstract.IncrementalTable{
			{Namespace: namespace, Name: tableName, CursorField: cursorField, InitialState: initialState},
		},
		IncrementDelaySeconds: incrementDelay,
		CronExpression:        "",
	}

	transfer := &model.Transfer{
		ID:              transferID,
		Type:            transferType,
		Src:             src,
		Dst:             dst,
		RegularSnapshot: regularSnapshot,
	}
	transfer.FillDependentFields()
	return transfer
}
