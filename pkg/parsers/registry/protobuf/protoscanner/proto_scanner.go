package protoscanner

import (
	"github.com/transferria/transferria/pkg/abstract"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type ProtoScannerType string

const (
	ScannerTypeLineSplitter = ProtoScannerType("lineSplitter")
	ScannerTypeRepeated     = ProtoScannerType("repeated")
)

type ProtoScanner interface {
	Scan() bool
	Message() (protoreflect.Message, error)
	RawData() []byte
	// Approximate raw data len
	ApxDataLen() int
	Err() error
}

func NewProtoScanner(scannerType ProtoScannerType, lineSplitter abstract.LfLineSplitter, data []byte, msgDesc protoreflect.MessageDescriptor) (ProtoScanner, error) {
	switch scannerType {
	case ScannerTypeLineSplitter:
		sc, err := NewSplitterScanner(lineSplitter, data, msgDesc)
		if err != nil {
			return sc, xerrors.Errorf("NewSplitterScanner: %v", err)
		}

		return sc, nil
	case ScannerTypeRepeated:
		sc, err := NewRepeatedScanner(data, msgDesc)
		if err != nil {
			return sc, xerrors.Errorf("NewRepeatedScanner: %v", err)
		}

		return sc, nil
	default:
		return nil, xerrors.Errorf("unknown proto msg scanner type: %s", string(scannerType))
	}
}
