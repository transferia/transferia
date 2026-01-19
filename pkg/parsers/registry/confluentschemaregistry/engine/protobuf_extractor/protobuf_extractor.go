package protobuf_extractor

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func ExtractMessageFullNameByIndex(protoSchema string, messageIndexes []int) (string, error) {
	fileDescr, err := buildToProtoreflectFileDescriptor(protoSchema)
	if err != nil {
		return "", xerrors.Errorf("failed to build to protoreflect file descriptor, err: %w", err)
	}
	currMsgs := fileDescr.Messages()
	var msgDescription protoreflect.MessageDescriptor
	for _, el := range messageIndexes {
		msgDescription = currMsgs.Get(el)
		if msgDescription == nil {
			return "", xerrors.Errorf("msgDescription is nil (1)")
		}
		currMsgs = msgDescription.Messages()
	}
	if msgDescription == nil {
		return "", xerrors.Errorf("msgDescription is nil (2)")
	}
	return string(msgDescription.FullName()), nil
}
