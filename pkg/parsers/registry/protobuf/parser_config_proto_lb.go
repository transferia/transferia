package protobuf

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	"github.com/transferia/transferia/pkg/parsers/resources"
	"go.ytsaurus.tech/library/go/core/log"
)

type ParserConfigProtoLb struct {
	DescFile         []byte
	DescResourceName string
	MessageName      string

	IncludeColumns []protoparser.ColParams
	PrimaryKeys    []string
	PackageType    protoparser.MessagePackageType

	NullKeysAllowed    bool
	AddSystemColumns   bool
	SkipDedupKeys      bool
	AddSyntheticKeys   bool
	NotFillEmptyFields bool
}

func (c *ParserConfigProtoLb) IsNewParserConfig() {}

func (c *ParserConfigProtoLb) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigProtoLb) Validate() error {
	return nil
}

func (c *ParserConfigProtoLb) ToProtoParserConfig(logger log.Logger) (*protoparser.ProtoParserConfig, error) {
	if len(c.DescFile) == 0 && len(c.DescResourceName) == 0 {
		return nil, xerrors.Errorf("desc file and desc resource are both empty")
	}

	descFileContent := c.DescFile
	if len(c.DescResourceName) > 0 {
		resourcesObj, err := resources.NewResources(logger, []string{c.DescResourceName})
		if err != nil {
			return nil, xerrors.Errorf("unable to get resources, err: %v", err)
		}

		res, err := resourcesObj.GetResource(c.DescResourceName)
		if err != nil {
			return nil, xerrors.Errorf("unable to get resources, err: %v", err)
		}

		descFileContent = []byte(res)
	}

	cfg := &protoparser.ProtoParserConfig{
		ProtoMessageDesc:   nil,
		ScannerMessageDesc: nil,
		ProtoScannerType:   "",
		IncludeColumns:     c.IncludeColumns,
		PrimaryKeys:        c.PrimaryKeys,
		LineSplitter:       "",
		TableSplitter:      nil,
		TimeField:          nil,
		NullKeysAllowed:    c.NullKeysAllowed,
		NotFillEmptyFields: c.NotFillEmptyFields,
		AddSyntheticKeys:   c.AddSyntheticKeys,
		AddSystemColumns:   c.AddSystemColumns,
		SkipDedupKeys:      c.SkipDedupKeys,
	}

	if err := cfg.SetDescriptors(descFileContent, c.MessageName, c.PackageType); err != nil {
		return nil, xerrors.Errorf("SetDescriptors error: %v", err)
	}

	cfg.SetLineSplitter(c.PackageType)
	cfg.SetScannerType(c.PackageType)

	return cfg, nil
}
