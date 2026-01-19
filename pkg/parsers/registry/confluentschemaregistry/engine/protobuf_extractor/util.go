package protobuf_extractor

import (
	"io"
	"os"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/transferia/transferia/cloud/dataplatform/schemaregistry/pkg/formats/protobuf"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

func buildToProtoreflectFileDescriptor(body string) (protoreflect.FileDescriptor, error) {
	fileName := "stub.proto"

	files, err := getFiles(fileName, body)
	if err != nil {
		return nil, xerrors.Errorf("failed to get files from schema, err: %w", err)
	}

	protoFile, err := files.FindFileByPath(fileName)
	if err != nil {
		return nil, xerrors.Errorf("failed to find proto file, err: %w", err)
	}
	return protoFile, nil
}

func getFiles(fileName string, data string) (*protoregistry.Files, error) {
	referenceSchemas := make(map[string]string)
	referenceSchemas[fileName] = data
	files, err := getRegistry(fileName, referenceSchemas)
	if err != nil {
		return nil, xerrors.Errorf("failed to get registry files, err: %w", err)
	}
	return files, err
}

var errCouldNotResolveSchema = xerrors.New("could not resolve schema")

func getRegistry(subjectName string, refs map[string]string) (*protoregistry.Files, error) {
	currParser := protoparse.Parser{ //nolint:exhaustruct
		ValidateUnlinkedFiles: true,
		Accessor: func(filename string) (io.ReadCloser, error) {
			var contents string
			if data, ok := refs[filename]; ok {
				contents = string(data)
			}
			if contents == "" {
				contents = protobuf.BuiltInDeps[filename]
			}
			if contents == "" {
				return nil, os.ErrNotExist
			}
			return io.NopCloser(strings.NewReader(contents)), nil
		},
	}
	fds, err := currParser.ParseFiles(subjectName)
	if err == nil {
		if len(fds) != 1 {
			return nil, errCouldNotResolveSchema
		}
		result, err := protodesc.NewFiles(desc.ToFileDescriptorSet(fds...))
		if err != nil {
			return nil, xerrors.Errorf("failed to parse files, err: %w", err)
		}
		return result, nil
	}
	// try to build unlinked fileDescriptor proto for unknown extensions case
	var imports []string
	for key := range refs {
		imports = append(imports, key)
	}
	for key := range protobuf.BuiltInDeps {
		imports = append(imports, key)
	}
	unlinkedFiles, err := currParser.ParseFilesButDoNotLink(imports...)
	if err != nil {
		return nil, xerrors.Errorf("Unable to parse unlinked files %w", err)
	}
	result, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: unlinkedFiles})
	if err != nil {
		return nil, xerrors.Errorf("unable to parse unlinked files, err: %w", err)
	}
	return result, nil
}
