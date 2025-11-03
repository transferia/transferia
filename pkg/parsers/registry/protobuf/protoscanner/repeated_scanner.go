package protoscanner

import (
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type RepeatedScanner struct {
	items      protoreflect.List
	idx        int
	avgDataLen int

	gotMessage protoreflect.Message
}

func NewRepeatedScanner(data []byte, wrapperDesc protoreflect.MessageDescriptor) (*RepeatedScanner, error) {
	protoMsg := dynamicpb.NewMessage(wrapperDesc)

	if err := proto.Unmarshal(data, protoMsg); err != nil {
		return nil, xerrors.Errorf("error unmarshaling data: %v", err)
	}

	fields := wrapperDesc.Fields()
	if fields.Len() != 1 {
		return nil, xerrors.Errorf("wrong message descriptor: fields len = %d, expected 1", fields.Len())
	}

	fd := fields.Get(0)
	if !fd.IsList() {
		return nil, xerrors.Errorf(
			"wrong message descriptor: expected %s is list of message, got %s",
			fd.TextName(),
			fd.Kind().String(),
		)
	}

	if fd.Kind() != protoreflect.MessageKind {
		return nil, xerrors.Errorf(
			"wrong message descriptor: expected %s is list of message, got list of %s",
			fd.TextName(),
			fd.Kind().String(),
		)
	}

	avgDataLen := 0
	if amountMsg := protoMsg.Get(fd).List().Len(); amountMsg > 0 {
		avgDataLen = len(data) / amountMsg
	}

	return &RepeatedScanner{
		items:      protoMsg.Get(fd).List(),
		idx:        -1,
		avgDataLen: avgDataLen,
		gotMessage: nil,
	}, nil
}

func (s *RepeatedScanner) Scan() (res bool) {
	s.idx++
	if s.idx < 0 || s.idx >= s.items.Len() {
		return false
	}

	s.gotMessage = s.items.Get(s.idx).Message()
	return true
}

func (s *RepeatedScanner) Message() (protoreflect.Message, error) {
	if s.gotMessage == nil {
		return nil, xerrors.Errorf("message was not scanned")
	}

	return s.gotMessage, nil
}

func (s *RepeatedScanner) RawData() []byte {
	if s.gotMessage == nil {
		return nil
	}

	data, err := proto.Marshal(s.gotMessage.Interface())
	if err != nil {
		return nil
	}

	return data
}

func (s *RepeatedScanner) ApxDataLen() int {
	return s.avgDataLen
}

func (s *RepeatedScanner) Err() error {
	return nil
}
