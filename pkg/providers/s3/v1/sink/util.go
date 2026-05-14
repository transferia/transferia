package sink

import (
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	"github.com/transferia/transferia/pkg/serializer"
	"github.com/transferia/transferia/pkg/util"
)

type buckets map[string]map[string]*FileCache

const maxPartIDLen = 24

func RowFqtn(tableID abstract.TableID) string {
	if tableID.Namespace != "" {
		return fmt.Sprintf("%v_%v", tableID.Namespace, tableID.Name)
	}
	return tableID.Name
}

func rowPart(row abstract.ChangeItem) string {
	if row.IsMirror() {
		return fmt.Sprintf("%v_%v", row.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageTopic]], row.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessagePartition]])
	}
	res := RowFqtn(row.TableID())
	if row.PartID != "" {
		res = fmt.Sprintf("%s_%s", res, hashLongPart(row.PartID, maxPartIDLen))
	}
	return res
}

func CreateSerializer(s s3_v1_model.SerializerConfig) (serializer.BatchSerializer, error) {
	config := s.AsConfig()

	batchSerializer := serializer.NewBatchSerializer(config)
	if batchSerializer == nil {
		return nil, xerrors.New("s3_sink: Unsupported format")
	}

	return batchSerializer, nil
}

func hashLongPart(text string, maxLen int) string {
	if len(text) < maxLen {
		return text
	}
	return util.Hash(text)
}

func countInsertItems(input []abstract.ChangeItem) int {
	count := 0
	for _, item := range input {
		if item.Kind == abstract.InsertKind {
			count++
		}
	}
	return count
}
