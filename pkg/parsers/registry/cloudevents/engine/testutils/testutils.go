package testutils

import (
	"strings"
	"testing"

	"github.com/cloudevents/sdk-go/binding/format/protobuf/v2/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func ChangeRegistryURL(t *testing.T, buf []byte, newURL string) []byte {
	protoMsg := &pb.CloudEvent{}
	err := proto.Unmarshal(buf, protoMsg)
	require.NoError(t, err)

	for k, v := range protoMsg.Attributes {
		if k == "dataschema" {
			oldSchema := v.Attr.(*pb.CloudEventAttributeValue_CeUri).CeUri
			v.Attr.(*pb.CloudEventAttributeValue_CeUri).CeUri = strings.ReplaceAll(oldSchema, "http://localhost:8081", newURL)
		}
	}

	res, err := proto.Marshal(protoMsg)
	require.NoError(t, err)
	return res
}
