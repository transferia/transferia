package engine

import (
	_ "embed"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/binding/format/protobuf/v2/pb"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/changeitem/strictify"
	"github.com/transferria/transferria/pkg/parsers"
	"github.com/transferria/transferria/pkg/parsers/registry/cloudevents/engine/testutils"
	confluentsrmock "github.com/transferria/transferria/tests/helpers/confluent_schema_registry_mock"
	"google.golang.org/protobuf/proto"
)

var idToBuf = make(map[int]string)
var messagesData [][]byte

//go:embed testdata/test_schemas.json
var jsonSchemas []byte

//go:embed testdata/topic-profile.bin
var topicProfile []byte

//go:embed testdata/topic-shot.bin
var topicShot []byte

//go:embed testdata/message-name-from-any.bin
var messageNameFromAny []byte

func init() {
	var name map[string]interface{}
	_ = json.Unmarshal(jsonSchemas, &name)
	for kStr, vObj := range name {
		k, _ := strconv.Atoi(kStr)
		v, _ := json.Marshal(vObj)
		idToBuf[k] = string(v)
	}

	messagesData = append(messagesData, topicProfile)
	messagesData = append(messagesData, topicShot)
	messagesData = append(messagesData, messageNameFromAny)
}

func makePersqueueReadMessage(i int, rawLine []byte) parsers.Message {
	return parsers.Message{
		Offset:     uint64(i),
		SeqNo:      0,
		Key:        []byte("test_source_id"),
		CreateTime: time.Date(2010, time.October, 10, 10, 10, 10, 10, time.UTC),
		WriteTime:  time.Date(2020, time.February, 2, 20, 20, 20, 20, time.UTC),
		Value:      rawLine,
		Headers:    map[string]string{"some_field": "test"},
	}
}

func TestClient(t *testing.T) {
	schemaRegistryMock := confluentsrmock.NewConfluentSRMock(idToBuf, nil)
	defer schemaRegistryMock.Close()

	parser := NewCloudEventsImpl("", "uname", "pass", "", false, logger.Log, nil)
	var canonArr []abstract.ChangeItem
	for i, data := range messagesData {
		if len(data) == 0 {
			continue
		}
		result := parser.Do(makePersqueueReadMessage(i, testutils.ChangeRegistryURL(t, data, schemaRegistryMock.URL())), abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		for j := range result {
			// get back original sr uri
			uri := strings.Split(result[j].ColumnValues[3].(string), "/schemas")
			result[j].ColumnValues[3] = "http://localhost:8081/schemas" + uri[1]
		}
		canonArr = append(canonArr, result...)
		abstract.Dump(result)
	}
	canon.SaveJSON(t, canonArr)
	for _, item := range canonArr {
		require.NoError(t, strictify.Strictify(&item, abstract.MakeFastTableSchema(item.TableSchema.Columns())))
	}
}

func removeTime(t *testing.T, buf []byte) []byte {
	protoMsg := &pb.CloudEvent{}
	err := proto.Unmarshal(buf, protoMsg)
	require.NoError(t, err)

	oldLen := len(protoMsg.Attributes)
	delete(protoMsg.Attributes, "time")
	require.Equal(t, oldLen-1, len(protoMsg.Attributes))

	res, err := proto.Marshal(protoMsg)
	require.NoError(t, err)
	return res
}

func makeTimeWrongType(t *testing.T, buf []byte) []byte {
	protoMsg := &pb.CloudEvent{}
	err := proto.Unmarshal(buf, protoMsg)
	require.NoError(t, err)

	val := &pb.CloudEventAttributeValue_CeString{
		CeString: "blablabla",
	}
	protoMsg.Attributes["time"].Attr = val

	res, err := proto.Marshal(protoMsg)
	require.NoError(t, err)
	return res
}

func TestTimeAbsentOrWrongType(t *testing.T) {
	schemaRegistryMock := confluentsrmock.NewConfluentSRMock(idToBuf, nil)
	defer schemaRegistryMock.Close()

	parser := NewCloudEventsImpl("", "uname", "pass", "", false, logger.Log, nil)

	for i, data := range messagesData {
		if len(data) == 0 {
			continue
		}
		rawLine := removeTime(t, testutils.ChangeRegistryURL(t, data, schemaRegistryMock.URL()))
		result := parser.Do(makePersqueueReadMessage(i, rawLine), abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		require.True(t, parsers.IsUnparsed(result[0]), "ChangeItem should be unparsed")
	}

	for i, data := range messagesData {
		if len(data) == 0 {
			continue
		}
		rawLine := makeTimeWrongType(t, testutils.ChangeRegistryURL(t, data, schemaRegistryMock.URL()))
		result := parser.Do(makePersqueueReadMessage(i, rawLine), abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		require.True(t, parsers.IsUnparsed(result[0]), "ChangeItem should be unparsed")
	}
}
