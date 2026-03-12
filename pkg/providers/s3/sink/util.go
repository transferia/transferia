package sink

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/serializer"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/xlocale"
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

func CreateSerializer(outputFormat model.ParsingFormat, anyAsString bool) (serializer.BatchSerializer, error) {
	config := &serializer.BatchSerializerCommonConfig{
		UnsupportedItemKinds: nil,
		Format:               outputFormat,
		AddClosingNewLine:    true,
		AnyAsString:          anyAsString,
		CompressionCodec:     nil, // TODO: add compression codec for parquet
	}

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

func extractRowBucket(row abstract.ChangeItem, layoutColumn string, layout string) string {
	rowBucketTime := time.Unix(0, int64(row.CommitTime))
	if layoutColumn != "" {
		rowBucketTime = model.ExtractTimeCol(row, layoutColumn)
	}
	if layout != "" {
		if loc, err := xlocale.Load(layout); err == nil {
			rowBucketTime = rowBucketTime.In(loc)
		}
	}
	return rowBucketTime.Format(layout)
}
