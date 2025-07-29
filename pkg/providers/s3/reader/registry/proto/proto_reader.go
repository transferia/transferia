package proto

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	"github.com/transferia/transferia/pkg/providers/s3"
	abstract_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	abstract_reader.RegisterReader(model.ParsingFormatPROTO, NewProtoReader)
}

func NewProtoReader(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (abstract_reader.Reader, error) {
	if len(src.Format.ProtoParser.DescFile) == 0 {
		return nil, xerrors.New("desc file required")
	}
	// this is magic field to get descriptor from YT
	if len(src.Format.ProtoParser.DescResourceName) != 0 {
		return nil, xerrors.New("desc resource name is not supported by S3 source")
	}
	cfg := new(protoparser.ProtoParserConfig)
	cfg.IncludeColumns = src.Format.ProtoParser.IncludeColumns
	cfg.PrimaryKeys = src.Format.ProtoParser.PrimaryKeys
	cfg.NullKeysAllowed = src.Format.ProtoParser.NullKeysAllowed
	if err := cfg.SetDescriptors(
		src.Format.ProtoParser.DescFile,
		src.Format.ProtoParser.MessageName,
		src.Format.ProtoParser.PackageType,
	); err != nil {
		return nil, xerrors.Errorf("SetDescriptors error: %v", err)
	}
	cfg.SetLineSplitter(src.Format.ProtoParser.PackageType)
	cfg.SetScannerType(src.Format.ProtoParser.PackageType)

	parser, err := protoparser.NewProtoParser(cfg, metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct proto parser: %w", err)
	}
	reader, err := abstract_reader.NewGenericParserReader(
		src,
		lgr,
		sess,
		metrics,
		parser,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to initialize new generic reader: %w", err)
	}
	return reader, nil
}
