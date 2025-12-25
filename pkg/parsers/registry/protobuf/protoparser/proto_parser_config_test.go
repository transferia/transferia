package protoparser

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/yatest"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func protoSampleContent(t *testing.T, filename string) []byte {
	fp := yatest.SourcePath(filepath.Join("transfer_manager/go/pkg/parsers/registry/protobuf/protoparser/gotest/", filename))
	fileContent, err := os.ReadFile(fp)
	require.NoError(t, err)
	require.Positive(t, len(fileContent))
	return fileContent
}

func Test_extractMessageDesc(t *testing.T) {
	descFileContent := protoSampleContent(t, "extract_message.desc")

	msgDesc, err := extractMessageDesc(descFileContent, "MessageOne")
	require.NoError(t, err)
	require.Equal(t, 7, msgDesc.Fields().Len())

	require.Equal(t, protoreflect.StringKind, msgDesc.Fields().ByTextName("string_field").Kind())
	require.Equal(t, protoreflect.Int32Kind, msgDesc.Fields().ByTextName("int32_field").Kind())
	require.Equal(t, protoreflect.EnumKind, msgDesc.Fields().ByTextName("enum_field").Kind())
	require.Equal(t, protoreflect.MessageKind, msgDesc.Fields().ByTextName("message_two").Kind())
	require.True(t, msgDesc.Fields().ByTextName("repeated_field").IsList())

	msgDesc, err = extractMessageDesc(descFileContent, "MessageTwo")
	require.NoError(t, err)
	require.Equal(t, 3, msgDesc.Fields().Len())
}
