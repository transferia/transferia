package protobuf_extractor

import (
	"strings"

	vanilla_confluent "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent"
	confluent_types "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoprint"
	"github.com/transferia/transferia/internal/logger"
	"go.ytsaurus.tech/library/go/core/log"
	google_api "google.golang.org/genproto/googleapis/api"
	"google.golang.org/genproto/googleapis/api/annotations"
	google_http "google.golang.org/genproto/googleapis/rpc/http"
	"google.golang.org/genproto/googleapis/type/calendarperiod"
	"google.golang.org/genproto/googleapis/type/color"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/datetime"
	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/genproto/googleapis/type/fraction"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/genproto/googleapis/type/money"
	"google.golang.org/genproto/googleapis/type/month"
	"google.golang.org/genproto/googleapis/type/postaladdress"
	"google.golang.org/genproto/googleapis/type/quaternion"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/apipb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/sourcecontextpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/typepb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func init() {
	// can be taken from:
	// https://github.com/confluentinc/confluent-kafka-go/blob/master/schemaregistry/serde/protobuf/protobuf.go#L85
	//
	// + somewhere we take "google/api/..." & "google/rpc/..."
	builtins := map[string]protoreflect.FileDescriptor{
		// basic imports list from kafka library
		"confluent/meta.proto":                 vanilla_confluent.File_schemaregistry_confluent_meta_proto,
		"confluent/type/decimal.proto":         confluent_types.File_schemaregistry_confluent_type_decimal_proto,
		"google/type/calendar_period.proto":    calendarperiod.File_google_type_calendar_period_proto,
		"google/type/color.proto":              color.File_google_type_color_proto,
		"google/type/date.proto":               date.File_google_type_date_proto,
		"google/type/datetime.proto":           datetime.File_google_type_datetime_proto,
		"google/type/dayofweek.proto":          dayofweek.File_google_type_dayofweek_proto,
		"google/type/expr.proto":               expr.File_google_type_expr_proto,
		"google/type/fraction.proto":           fraction.File_google_type_fraction_proto,
		"google/type/latlng.proto":             latlng.File_google_type_latlng_proto,
		"google/type/money.proto":              money.File_google_type_money_proto,
		"google/type/month.proto":              month.File_google_type_month_proto,
		"google/type/postal_address.proto":     postaladdress.File_google_type_postal_address_proto,
		"google/type/quaternion.proto":         quaternion.File_google_type_quaternion_proto,
		"google/type/timeofday.proto":          timeofday.File_google_type_timeofday_proto,
		"google/protobuf/any.proto":            anypb.File_google_protobuf_any_proto,
		"google/protobuf/api.proto":            apipb.File_google_protobuf_api_proto,
		"google/protobuf/descriptor.proto":     descriptorpb.File_google_protobuf_descriptor_proto,
		"google/protobuf/duration.proto":       durationpb.File_google_protobuf_duration_proto,
		"google/protobuf/empty.proto":          emptypb.File_google_protobuf_empty_proto,
		"google/protobuf/field_mask.proto":     fieldmaskpb.File_google_protobuf_field_mask_proto,
		"google/protobuf/source_context.proto": sourcecontextpb.File_google_protobuf_source_context_proto,
		"google/protobuf/struct.proto":         structpb.File_google_protobuf_struct_proto,
		"google/protobuf/timestamp.proto":      timestamppb.File_google_protobuf_timestamp_proto,
		"google/protobuf/type.proto":           typepb.File_google_protobuf_type_proto,
		"google/protobuf/wrappers.proto":       wrapperspb.File_google_protobuf_wrappers_proto,
		// annotations for rpc service
		"google/api/annotations.proto":    annotations.File_google_api_annotations_proto,
		"google/rpc/http.proto":           google_http.File_google_rpc_http_proto,
		"google/api/client.proto":         annotations.File_google_api_client_proto,
		"google/api/http.proto":           annotations.File_google_api_http_proto,
		"google/api/field_behavior.proto": annotations.File_google_api_field_behavior_proto,
		"google/api/resource.proto":       annotations.File_google_api_resource_proto,
		"google/api/routing.proto":        annotations.File_google_api_routing_proto,
		"google/api/launch_stage.proto":   google_api.File_google_api_launch_stage_proto,
	}
	fds := make([]*descriptorpb.FileDescriptorProto, 0, len(builtins))
	for _, value := range builtins {
		fd := protodesc.ToFileDescriptorProto(value)
		fds = append(fds, fd)
	}
	fdMap, err := desc.CreateFileDescriptors(fds)
	if err != nil {
		logger.Log.Fatal("could not create file descriptors for builtin protos", log.NamedError("error", err))
	}
	printer := protoprint.Printer{ //nolint:exhaustruct
		PreferMultiLineStyleComments:           false,
		SortElements:                           true,
		CustomSortFunction:                     nil,
		Indent:                                 "",
		OmitComments:                           protoprint.CommentsAll,
		Compact:                                true,
		ForceFullyQualifiedNames:               true,
		TrailingCommentsOnSeparateLine:         false,
		ShortOptionsExpansionThresholdCount:    0,
		ShortOptionsExpansionThresholdLength:   0,
		MessageLiteralExpansionThresholdLength: 0,
	}
	BuiltInDeps = make(map[string]string)
	for key, value := range fdMap {
		var writer strings.Builder
		err = printer.PrintProtoFile(value, &writer)
		if err != nil {
			logger.Log.Fatal("could not print builtin proto file", log.String("key", key), log.NamedError("error", err))
		}
		// trim prefix for schemaregistry/confluent/meta.proto -> confluent/meta.proto
		BuiltInDeps[trimSRPrefix(key)] = writer.String()
	}
}

func trimSRPrefix(in string) string {
	return strings.TrimPrefix(in, "schemaregistry/")
}
