package engine

import (
	"strings"

	"github.com/cloudevents/sdk-go/binding/format/protobuf/v2/pb"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"google.golang.org/protobuf/proto"
)

func unpackCloudEventsProtoMessage(buf []byte) (*cloudEventsProtoFields, []byte, string, error) {
	result := new(cloudEventsProtoFields)

	protoMsg := &pb.CloudEvent{}
	err := proto.Unmarshal(buf, protoMsg)
	if err != nil {
		return nil, nil, "", xerrors.Errorf("unable to unmarshal protobuf, err: %w", err)
	}

	protoDataRaw := protoMsg.Data.(*pb.CloudEvent_ProtoData).ProtoData

	var protoPath string
	if strings.HasPrefix(protoDataRaw.TypeUrl, "type.googleapis.com/") {
		protoPath = strings.TrimPrefix(protoDataRaw.TypeUrl, "type.googleapis.com/")
	}
	protoData := protoDataRaw.Value

	timePresent := false
	dataSchema := ""
	for k, v := range protoMsg.Attributes {
		if k == "dataschema" {
			dataSchema = v.Attr.(*pb.CloudEventAttributeValue_CeUri).CeUri
		}

		if k == "time" {
			switch v.Attr.(type) {
			case *pb.CloudEventAttributeValue_CeTimestamp:
			default:
				return nil, nil, "", xerrors.Errorf("field 'time' should be 'ce_timestamp', but present: %T", v.Attr)
			}
			timestamp := v.Attr.(*pb.CloudEventAttributeValue_CeTimestamp).CeTimestamp
			if err := timestamp.CheckValid(); err != nil {
				return nil, nil, "", xerrors.Errorf("Provided cloud event timestamp is not valid: %v", err)
			}
			result.time = timestamp.AsTime()
			timePresent = true
		}

		if k == "subject" {
			result.subject = v.Attr.(*pb.CloudEventAttributeValue_CeString).CeString
		}
	}

	if !timePresent {
		return nil, nil, "", xerrors.Errorf("field 'time' is mandatory in yandex-cloudevents")
	}

	if dataSchema == "" {
		return nil, nil, "", xerrors.Errorf("unable to find attribute 'dataschema' into cloudevents message")
	}

	result.id = protoMsg.Id
	result.source = protoMsg.Source
	result.type_ = protoMsg.Type
	result.dataschema = dataSchema

	return result, protoData, protoPath, nil
}
