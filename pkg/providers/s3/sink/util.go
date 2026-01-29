package sink

import (
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/serializer"
	"github.com/transferia/transferia/pkg/util"
)

type buckets map[string]map[string]*FileCache

func rowFqtn(tableID abstract.TableID) string {
	if tableID.Namespace != "" {
		return fmt.Sprintf("%v_%v", tableID.Namespace, tableID.Name)
	}
	return tableID.Name
}

func rowPart(row abstract.ChangeItem) string {
	if row.IsMirror() {
		return fmt.Sprintf("%v_%v", row.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageTopic]], row.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessagePartition]])
	}
	res := rowFqtn(row.TableID())
	if row.PartID != "" {
		res = fmt.Sprintf("%s_%s", res, hashLongPart(row.PartID, 24))
	}
	return res
}

func createSerializer(outputFormat model.ParsingFormat, anyAsString bool) (serializer.BatchSerializer, error) {
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

func createSnapshotIOHolder(outputEncoding s3_provider.Encoding, outputFormat model.ParsingFormat, anyAsString bool) (*snapshotHolder, error) {
	uploadDone := make(chan error)
	var snapshot Snapshot
	if outputEncoding == s3_provider.GzipEncoding {
		snapshot = NewSnapshotGzip()
	} else {
		snapshot = NewSnapshotRaw()
	}
	batchSerializer, err := createSerializer(outputFormat, anyAsString)
	if err != nil {
		return nil, err
	}

	return &snapshotHolder{
		uploadDone: uploadDone,
		snapshot:   snapshot,
		serializer: batchSerializer,
	}, nil
}
