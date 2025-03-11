package protoscanner

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers/scanner"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type SplitterScanner struct {
	scanner scanner.EventScanner
	msgDesc protoreflect.MessageDescriptor
}

func NewSplitterScanner(lineSplitter abstract.LfLineSplitter, data []byte, msgDesc protoreflect.MessageDescriptor) (*SplitterScanner, error) {
	sc, err := scanner.NewScanner(lineSplitter, data)
	if err != nil {
		return nil, xerrors.Errorf("construct underlying scanner error: %v", err)
	}

	return &SplitterScanner{
		scanner: sc,
		msgDesc: msgDesc,
	}, nil
}

func (s *SplitterScanner) Scan() bool {
	return s.scanner.Scan()
}

func (s *SplitterScanner) Message() (protoreflect.Message, error) {
	data, err := s.scanner.Event()
	if err != nil {
		return nil, xerrors.Errorf("geting protoseq item error: %v", err)
	}

	protoMsg := dynamicpb.NewMessage(s.msgDesc)
	if err := proto.Unmarshal(data, protoMsg); err != nil {
		return nil, xerrors.Errorf("error unmarshaling protoseq item: %v", err)
	}

	return protoMsg, nil
}

func (s *SplitterScanner) RawData() []byte {
	data, _ := s.scanner.Event()
	return data
}

func (s *SplitterScanner) ApxDataLen() int {
	data, _ := s.scanner.Event()
	return len(data)
}

func (s *SplitterScanner) Err() error {
	return s.scanner.Err()
}
