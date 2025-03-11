package protoparser

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoscanner"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

type MessagePackageType string

var (
	PackageTypeProtoseq  = MessagePackageType("PackageTypeProtoseq")
	PackageTypeRepeated  = MessagePackageType("PackageTypeRepeated")
	PackageTypeSingleMsg = MessagePackageType("PackageTypeSingleMsg")
)

type ColParams struct {
	Name     string
	Required bool
}

func RequiredColumn(name string) ColParams {
	return ColParams{
		Name:     name,
		Required: true,
	}
}

func OptionalColumn(name string) ColParams {
	return ColParams{
		Name:     name,
		Required: false,
	}
}

type ProtoParserConfig struct {
	ProtoMessageDesc   protoreflect.MessageDescriptor
	ScannerMessageDesc protoreflect.MessageDescriptor
	ProtoScannerType   protoscanner.ProtoScannerType
	IncludeColumns     []ColParams
	PrimaryKeys        []string
	LineSplitter       abstract.LfLineSplitter
	TableSplitter      *abstract.TableSplitter
	TimeField          *abstract.TimestampCol
	NullKeysAllowed    bool
	AddSystemColumns   bool
	SkipDedupKeys      bool
	AddSyntheticKeys   bool
}

// SetDescriptors sets ProtoMessageDesc & ScannerMessageDesc
func (c *ProtoParserConfig) SetDescriptors(descFileContent []byte, messageName string, pkgType MessagePackageType) error {
	rootMsgDesc, err := extractMessageDesc(descFileContent, messageName)
	if err != nil {
		return xerrors.Errorf("error extracting message descriptor from desc file: %v", err)
	}

	embeddedMsgDesc, ok := extractEmbeddedRepeatedMsgDesc(rootMsgDesc)
	if pkgType == PackageTypeRepeated && !ok {
		return xerrors.Errorf("can't extract embedded repeated message descriptor while provided package type is repeated")
	}

	c.ScannerMessageDesc = rootMsgDesc

	if pkgType == PackageTypeRepeated {
		c.ProtoMessageDesc = embeddedMsgDesc
	} else {
		c.ProtoMessageDesc = rootMsgDesc
	}

	return nil
}

func (c *ProtoParserConfig) SetLineSplitter(pkgType MessagePackageType) {
	switch pkgType {
	case PackageTypeProtoseq:
		c.LineSplitter = abstract.LfLineSplitterProtoseq
	case PackageTypeRepeated, PackageTypeSingleMsg:
		c.LineSplitter = abstract.LfLineSplitterDoNotSplit
	}
}

func (c *ProtoParserConfig) SetScannerType(pkgType MessagePackageType) {
	switch pkgType {
	case PackageTypeRepeated:
		c.ProtoScannerType = protoscanner.ScannerTypeRepeated
	case PackageTypeProtoseq, PackageTypeSingleMsg:
		c.ProtoScannerType = protoscanner.ScannerTypeLineSplitter
	}
}

func extractMessageDesc(fileContent []byte, msgName string) (protoreflect.MessageDescriptor, error) {
	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(fileContent, &fds); err != nil {
		return nil, xerrors.Errorf("error unmarshaling proto desc file content: %v", err)
	}

	registry, err := protodesc.NewFiles(&fds)
	if err != nil {
		return nil, xerrors.Errorf("can't create new protoregistry files: %v", err)
	}

	var msgDesc protoreflect.MessageDescriptor
	registry.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		mD := fd.Messages().ByName(protoreflect.Name(msgName))
		if mD != nil {
			msgDesc = mD
			return false
		}

		return true
	})

	if msgDesc == nil {
		return nil, xerrors.Errorf("message with provided name '%s' not found", msgName)
	}

	return msgDesc, nil
}

func extractEmbeddedRepeatedMsgDesc(msgDesc protoreflect.MessageDescriptor) (protoreflect.MessageDescriptor, bool) {
	fields := msgDesc.Fields()
	if fields.Len() == 1 &&
		fields.Get(0).IsList() &&
		fields.Get(0).Kind() == protoreflect.MessageKind {
		return fields.Get(0).Message(), true
	}

	return nil, false
}
