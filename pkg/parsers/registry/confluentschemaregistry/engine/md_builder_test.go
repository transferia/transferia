package engine

import (
	"testing"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/parsers/registry/confluentschemaregistry/engine/testdata/references"
	"github.com/transferria/transferria/pkg/schemaregistry/confluent"
	"google.golang.org/protobuf/proto"
)

func checkMD(t *testing.T, md *desc.MessageDescriptor, buf []byte) {
	dynamicMessage := dynamic.NewMessage(md)
	err := dynamicMessage.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, "msg1blablabla", dynamicMessage.GetFieldByName("msg1_string_field"))
}

func TestMDBuilder(t *testing.T) {
	currMDBuilder := newMDBuilder()
	currSchema := &confluent.Schema{
		ID:         0,
		Schema:     `syntax = "proto3"; message my_msg1 {string msg1_string_field = 1;}`,
		SchemaType: confluent.PROTOBUF,
		References: nil,
	}

	var ptr *desc.MessageDescriptor
	var recordName string
	var err error

	msg1 := &references.MyMsg1{}
	msg1.Msg1StringField = "msg1blablabla"
	buf, err := proto.Marshal(msg1)
	require.NoError(t, err)

	//---

	require.Equal(t, 0, len(currMDBuilder.msgIDToMD))
	ptr, recordName, err = currMDBuilder.toMD(currSchema, nil, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(currMDBuilder.msgIDToMD))
	require.Equal(t, "my_msg1", recordName)

	checkMD(t, ptr, buf)

	require.Equal(t, 1, len(currMDBuilder.msgIDToMD))
	ptr, recordName, err = currMDBuilder.toMD(currSchema, nil, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(currMDBuilder.msgIDToMD))
	require.Equal(t, "my_msg1", recordName)

	checkMD(t, ptr, buf)
}
