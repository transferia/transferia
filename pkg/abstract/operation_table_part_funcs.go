package abstract

import (
	"fmt"
	"sort"

	"github.com/transferia/transferia/internal/logger"
	yslices "github.com/transferia/transferia/library/go/slices"
	"go.ytsaurus.tech/library/go/core/log"
)

func DumpTablePartsToLogs(parts []*OperationTablePart) {
	chunkSize := 1000
	for i := 0; i < len(parts); i += chunkSize {
		end := i + chunkSize
		if end > len(parts) {
			end = len(parts)
		}

		partsToDump := yslices.Map(parts[i:end], func(part *OperationTablePart) string {
			return part.String()
		})
		logger.Log.Info(fmt.Sprintf("Tables leastParts (shards) to copy [%v, %v]", i+1, end), log.Strings("leastParts", partsToDump))
	}
}

func SortTableParts(parts []*OperationTablePart) {
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].TableKey() != parts[j].TableKey() {
			return parts[i].TableKey() < parts[j].TableKey()
		}
		if parts[i].Offset != parts[j].Offset {
			return parts[i].Offset < parts[j].Offset
		}
		return parts[i].Filter < parts[j].Filter
	})
}

func SortAndDeduplicateTableParts(parts []*OperationTablePart) []*OperationTablePart {
	partsMap := map[string]*OperationTablePart{}
	for _, incomingPart := range parts {
		key := fmt.Sprintf("%s-%s-%v", incomingPart.TableKey(), incomingPart.Filter, incomingPart.Offset)
		if old, ok := partsMap[key]; ok && (incomingPart.CompletedRows <= old.CompletedRows &&
			incomingPart.ReadBytes <= old.ReadBytes &&
			(old.Completed || incomingPart.Completed == old.Completed)) {
			continue
		}
		partsMap[key] = incomingPart
	}

	newParts := []*OperationTablePart{}
	for _, part := range partsMap {
		newParts = append(newParts, part)
	}

	SortTableParts(newParts)
	return newParts
}
