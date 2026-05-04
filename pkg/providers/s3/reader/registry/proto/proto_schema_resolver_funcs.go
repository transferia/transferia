package proto

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/stats"
)

func newProtoParserBuilder(src *s3_model.S3Source, metrics *stats.SourceStats) (parsers.ParserBuilder, error) {
	if len(src.Format.ProtoParser.DescFile) == 0 {
		return nil, xerrors.New("DescFile is empty")
	}
	if len(src.Format.ProtoParser.DescResourceName) != 0 {
		return nil, xerrors.New("DescResourceName is empty")
	}
	cfg := new(protoparser.ProtoParserConfig)
	cfg.IncludeColumns = src.Format.ProtoParser.IncludeColumns
	cfg.PrimaryKeys = src.Format.ProtoParser.PrimaryKeys
	cfg.NullKeysAllowed = src.Format.ProtoParser.NullKeysAllowed
	err := cfg.SetDescriptors(
		src.Format.ProtoParser.DescFile,
		src.Format.ProtoParser.MessageName,
		src.Format.ProtoParser.PackageType,
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to set descriptors, err: %w", err)
	}
	cfg.SetLineSplitter(src.Format.ProtoParser.PackageType)
	cfg.SetScannerType(src.Format.ProtoParser.PackageType)

	return protoparser.NewLazyProtoParserBuilder(cfg, metrics)
}
