package engine

import (
	_ "embed"
	"testing"

	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/parsers/registry/confluentschemaregistry/engine/testdata/types_protobuf_test_data"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed testdata/types_protobuf_test_data/std_data_types.proto
var stdDataTypesProto []byte

func TestUnpackVal(t *testing.T) {
	stdDataTypesFilled := &types_protobuf_test_data.StdDataTypesMsg{
		DoubleField:   1.11,
		FloatField:    2.2,
		Int32Field:    2,
		Int64Field:    3,
		Uint32Field:   4,
		Uint64Field:   5,
		Sint32Field:   6,
		Sint64Field:   7,
		Fixed32Field:  8,
		Fixed64Field:  9,
		Sfixed32Field: 10,
		Sfixed64Field: 11,
		BoolField:     true,
		StringField:   "string",
		BytesField:    []byte("bytes"),
		MapField: map[string]int32{
			"key1": 23,
			"key2": 12,
		},
		RepeatedField: []string{"1", "2", "3"},
		MsgField: &types_protobuf_test_data.EmbeddedMsg{
			StringField: "stringField",
			Int32Field:  2,
			EnumField:   types_protobuf_test_data.EmbeddedEnum_ITEM_2,
		},
		RepeatedMessage: []*types_protobuf_test_data.StdDataTypesMsgRepeatedMessage{
			{
				Type: "string",
				Status: &types_protobuf_test_data.EmbeddedMsg{
					StringField: "stringField2",
					Int32Field:  3,
					EnumField:   types_protobuf_test_data.EmbeddedEnum_ITEM_1,
				},
				UpdatedAt: &timestamppb.Timestamp{
					Seconds: 1234565,
					Nanos:   32141,
				},
			},
		},
	}
	data, err := proto.Marshal(stdDataTypesFilled)
	require.NoError(t, err)

	// make dyn message

	stubProto := "stub.proto"
	p := protoparse.Parser{
		Accessor: protoparse.FileContentsFromMap(protoMap(stubProto, string(stdDataTypesProto))),
	}
	fds, err := p.ParseFiles(stubProto)
	require.NoError(t, err)
	require.Len(t, fds, 1)

	fd := fds[0]

	md := fd.FindMessage("StdDataTypesMsg")
	dynamicMessage := dynamic.NewMessage(md)

	// parse dyn message

	err = dynamicMessage.Unmarshal(data)
	require.NoError(t, err)

	schema, names, vals, err := unpackProtobufDynamicMessage("", "", dynamicMessage)
	require.NoError(t, err)

	canon.SaveJSON(t, struct {
		Schema *abstract.TableSchema
		Names  []string
		Vals   []interface{}
	}{
		Schema: schema,
		Names:  names,
		Vals:   vals,
	})
}
