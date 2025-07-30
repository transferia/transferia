package engine

import (
	_ "embed"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"github.com/transferia/transferia/pkg/parsers"
	confluentsrmock "github.com/transferia/transferia/tests/helpers/confluent_schema_registry_mock"
)

var idToBuf = make(map[int]string)
var messagesData [][]byte

//go:embed testdata/test_schemas.json
var jsonSchemas []byte

//go:embed testdata/test_raw_json_messages
var jsonData []byte

//go:embed testdata/test_protobuf_0.bin
var protobuf0Data []byte

//go:embed testdata/test_protobuf_1.bin
var protobuf1Data []byte

func init() {
	var name map[string]interface{}
	_ = json.Unmarshal(jsonSchemas, &name)
	for kStr, vObj := range name {
		k, _ := strconv.Atoi(kStr)
		v, _ := json.Marshal(vObj)
		idToBuf[k] = string(v)
	}

	messagesDataArrStr := strings.Split(string(jsonData), "\n")
	for _, el := range messagesDataArrStr {
		messagesData = append(messagesData, []byte(el))
	}
	messagesData = append(messagesData, protobuf0Data)
	messagesData = append(messagesData, protobuf1Data)
}

func makePersqueueReadMessage(i int, rawLine []byte) parsers.Message {
	return parsers.Message{
		Offset:     uint64(i),
		SeqNo:      0,
		Key:        []byte("test_source_id"),
		CreateTime: time.Time{},
		WriteTime:  time.Time{},
		Value:      rawLine,
		Headers:    map[string]string{"some_field": "test"},
	}
}

func TestProtoMap(t *testing.T) {
	myMap := protoMap("k", "v")
	require.NotEqual(t, "", myMap["k"])
	require.NotEqual(t, "", myMap["confluent/meta.proto"])
	require.NotEqual(t, "", myMap["confluent/type/decimal.proto"])
	require.NotEqual(t, "", myMap["google/type/datetime.proto"])
}

func TestClient(t *testing.T) {
	schemaRegistryMock := confluentsrmock.NewConfluentSRMock(idToBuf, nil)
	defer schemaRegistryMock.Close()

	parser := NewConfluentSchemaRegistryImpl(schemaRegistryMock.URL(), "", "uname", "pass", false, false, logger.Log)
	var canonArr []abstract.ChangeItem
	for i, data := range messagesData {
		if len(data) == 0 {
			continue
		}
		result := parser.Do(makePersqueueReadMessage(i, data), abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
		switch i {
		case 0:
			require.Len(t, result, 2)
		default:
			require.Len(t, result, 1)
		}
		canonArr = append(canonArr, result...)
		abstract.Dump(result)
	}
	canon.SaveJSON(t, canonArr)
	for _, item := range canonArr {
		require.NoError(t, strictify.Strictify(&item, abstract.MakeFastTableSchema(item.TableSchema.Columns())))
	}
}
