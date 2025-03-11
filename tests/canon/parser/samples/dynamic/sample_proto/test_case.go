package sampleproto

import (
	"bytes"

	"github.com/golang/protobuf/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf"
	"github.com/transferia/transferia/tests/canon/parser/samples/dynamic/sample_proto/sample_proto"
	"github.com/transferia/transferia/tests/canon/parser/testcase"
	"go.ytsaurus.tech/library/go/core/log"
	"google.golang.org/protobuf/proto"
)

func makeDesc() ([]byte, error) {
	messageDescriptorProto, _ := descriptor.MessageDescriptorProto(new(sample_proto.SampleProto))
	fd, err := desc.CreateFileDescriptor(messageDescriptorProto)
	if err != nil {
		return nil, xerrors.Errorf("cannot create file descriptor: %w", err)
	}
	ffds := desc.ToFileDescriptorSet(fd)
	data, err := proto.Marshal(ffds)
	if err != nil {
		return nil, xerrors.Errorf("cannot marshal file descriptor: %w", err)
	}
	return data, nil
}

func buildMessages() []proto.Message {
	return []proto.Message{
		&sample_proto.SampleProto{
			DoubleField:   3.14,
			FloatField:    1.46,
			Int32Field:    -228,
			Int64Field:    -1337,
			Uint32Field:   42,
			Uint64Field:   300000000000,
			Sint32Field:   -1,
			Sint64Field:   -1488,
			Fixed32Field:  9001,
			Fixed64Field:  127,
			Sfixed32Field: -5,
			Sfixed64Field: -273,
			BoolField:     true,
			StringField:   "Я знаю, что ты знаешь этот трек, готовься подпевать",
			BytesField:    []byte("Раз, два, три, четыре"),
			MapField: map[string]int32{
				"МУЗЫКА ГРОМЧЕ, ГЛАЗА ЗАКРЫТЫ":       1,
				"ЭТО НОНСТОП, НОЧЬЮ ОТКРЫТИЙ":        1,
				"ДЕЛАЙ ЧТО ХОЧЕШЬ, Я ЗАБЫВАЮСЬ":      1,
				"ЭТО НОНСТОП, НЕ ПРЕКРАЩАЯСЬ":        1,
				"МУЗЫКА ГРОМЧЕ, ГЛАЗА ЗАКРЫТЫ (хэй)": 2,
				"ЭТО НОНСТОООП, НОЧЬЮ ОТКРЫТИЙ":      2,
				"БУДУ С ТОБОЮ, САМОЙ ПРИМЕРНОЮ":      2,
				"УТРО В ОКНЕ И МЫ БУДЕМ ПЕРВЫМИ":     2,
			},
			RepeatedField: []string{
				"Этот трек делает тебя сильней",
				"Он прикольней, чем колёса, и роднее, чем портвейн",
				"Раза в три круче, чем самый первый секс-партнёр",
				"Все девчата в таком трипе, что аж «мама не горюй», йоп",
			},
			MsgField: &sample_proto.SampleEmbeddedMsg{
				StringField: "Прыгай в такт, прыгай в такт, будто ты совсем дурак",
				Int32Field:  256,
				EnumField:   sample_proto.SampleEmbeddedEnum_ITEM_2,
			},
		},
	}
}

func makeData() []byte {
	var out bytes.Buffer
	for i, msg := range buildMessages() {
		data, err := proto.Marshal(msg)
		if err != nil {
			logger.Log.Fatal("Cannot marshall message", log.Any("iteration", i), log.Error(err))
		}
		out.Write(data)
	}
	return out.Bytes()
}

func makeParser() *protobuf.ParserConfigProtoLb {
	descFile, err := makeDesc()
	if err != nil {
		panic(err)
	}
	return &protobuf.ParserConfigProtoLb{
		DescFile:         descFile,
		DescResourceName: "",
		MessageName:      "SampleProto",
		IncludeColumns:   nil,
		PrimaryKeys:      nil,
		PackageType:      "PackageTypeSingleMsg",
		NullKeysAllowed:  false,
		AddSystemColumns: false,
		SkipDedupKeys:    false,
		AddSyntheticKeys: false,
	}
}

func MakeTestCaseSampleProto() testcase.TestCase {
	return testcase.TestCase{
		TopicName:    "/market-loyalty/testing/promos/market-perks",
		ParserConfig: makeParser(),
		Data:         testcase.MakeDefaultPersqueueReadMessage(makeData()),
	}
}
