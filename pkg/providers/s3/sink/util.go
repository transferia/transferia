package sink

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
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

func CreateSerializer(outputFormat model.ParsingFormat, anyAsString bool, parquetSetting *s3_model.ParquetSerializerSettings) (serializer.BatchSerializer, error) {
	var parquetConfig serializer.ParquetBatchSerializerConfig
	if parquetSetting != nil {
		rowGroupMaxRows := parquetSetting.RowGroupMaxRows
		rowGroupMaxBytes := parquetSetting.RowGroupMaxBytes
		if rowGroupMaxRows < 0 {
			rowGroupMaxRows = 0
		}
		if rowGroupMaxBytes < 0 {
			rowGroupMaxBytes = 0
		}
		parquetConfig = serializer.ParquetBatchSerializerConfig{
			CompressionCodec: serializer.CodecFromString(parquetSetting.CompressionCodec),
			RowGroupMaxRows:  uint64(rowGroupMaxRows),
			RowGroupMaxBytes: uint64(rowGroupMaxBytes),
		}
	}
	config := &serializer.BatchSerializerCommonConfig{
		UnsupportedItemKinds: nil,
		Format:               outputFormat,
		AddClosingNewLine:    true,
		AnyAsString:          anyAsString,
		ParquetConfig:        &parquetConfig,
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

// isDynamicLayout reports whether layout is a Go time-format template containing
// at least one format directive.
// An empty layout is treated as static.
func isDynamicLayout(layout string) bool {
	if layout == "" {
		return false
	}
	t1 := time.Unix(0, 0).UTC()
	t2 := time.Unix(1<<32, 0).UTC()
	return t1.Format(layout) != t2.Format(layout)
}

func makeListPrefix(layout string, basePath string) string {
	switch {
	case isDynamicLayout(layout):
		return ""
	case layout == "":
		return basePath
	default:
		return layout + "/" + basePath
	}
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
