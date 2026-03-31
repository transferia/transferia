//go:build !persqueue

package protobuf_extractor

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func buildToProtoreflectFileDescriptor(_ string) (protoreflect.FileDescriptor, error) {
	return nil, xerrors.New("schema registry protobuf support is not enabled: build with -tags persqueue")
}
