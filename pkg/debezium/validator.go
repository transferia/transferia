package debezium

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	confluentsrmock "github.com/transferia/transferia/tests/helpers/confluent_schema_registry_mock"
)

func panicOnError(err error) {
	if err != nil {
		panic("!")
	}
}

func validateDocument(schemaText string, document interface{}) {
	c := jsonschema.NewCompiler()
	c.Draft = jsonschema.Draft7
	err := c.AddResource("./main.json", strings.NewReader(schemaText))
	panicOnError(err)
	schema, err := c.Compile("./main.json")
	panicOnError(err)
	err = schema.Validate(document)
	panicOnError(err)
}

func emitAndValidate(connectorParameters map[string]string, changeItem *abstract.ChangeItem, additionalParams map[string]string) {
	mu := sync.Mutex{}
	bodyToSchemaID := make(map[string]int)
	handler := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		buf := new(strings.Builder)
		_, err := io.Copy(buf, r.Body)
		panicOnError(err)
		requestStr := buf.String()

		type request struct {
			Schema string `json:"schema"`
		}
		var req request
		err = json.Unmarshal([]byte(requestStr), &req)
		panicOnError(err)

		if _, ok := bodyToSchemaID[req.Schema]; ok {
			return
		}
		bodyToSchemaID[req.Schema] = len(bodyToSchemaID) + 1
	}
	sr := confluentsrmock.NewConfluentSRMock(nil, handler)
	defer sr.Close()
	params := make(map[string]string)
	for k, v := range connectorParameters {
		params[k] = v
	}
	srParams := map[string]string{
		debeziumparameters.KeyConverter:                    debeziumparameters.ConverterConfluentJSON,
		debeziumparameters.KeyConverterSchemaRegistryURL:   sr.URL(),
		debeziumparameters.KeyConverterBasicAuthUserInfo:   "my_login:my_pass",
		debeziumparameters.ValueConverter:                  debeziumparameters.ConverterConfluentJSON,
		debeziumparameters.ValueConverterSchemaRegistryURL: sr.URL(),
		debeziumparameters.ValueConverterBasicAuthUserInfo: "my_login:my_pass",
	}
	for k, v := range srParams {
		params[k] = v
	}
	for k, v := range additionalParams {
		params[k] = v
	}

	dropKeys := debeziumparameters.GetBatchingMaxSize(params) != 0

	emitter, err := NewMessagesEmitter(params, "1.1.2.Final", dropKeys, logger.Log)
	panicOnError(err)
	emitter.TestSetIgnoreUnknownSources(true)
	currDebeziumKV, err := emitter.emitKV(changeItem, time.Time{}, true, nil)
	panicOnError(err)

	idToSchemaBody := make(map[int]string)
	for k, v := range bodyToSchemaID {
		idToSchemaBody[v] = k
	}
	if len(bodyToSchemaID) != len(idToSchemaBody) {
		panicOnError(xerrors.New("!"))
	}

	check := func(inStr string) {
		in := []byte(inStr)
		schemaID := binary.BigEndian.Uint32([]byte(in)[1:5])
		schema := idToSchemaBody[int(schemaID)]
		docBytes := []byte(in)[5:]

		fmt.Println("EXTRA_VALIDATION_ON_TESTS__DEBEZIUM:schema:", schema)
		fmt.Println("EXTRA_VALIDATION_ON_TESTS__DEBEZIUM:doc:", string(docBytes))

		var docObj interface{}
		err := json.Unmarshal(docBytes, &docObj)
		panicOnError(err)
		validateDocument(schema, docObj)
	}

	for _, el := range currDebeziumKV {
		if !dropKeys {
			check(el.DebeziumKey)
		}
		if el.DebeziumVal != nil {
			check(*el.DebeziumVal)
		}
	}
}

func validateOnTests(connectorParameters map[string]string, changeItem *abstract.ChangeItem) {
	emitAndValidate(connectorParameters, changeItem, map[string]string{"decimal.handling.mode": "precise"})
	emitAndValidate(connectorParameters, changeItem, map[string]string{"decimal.handling.mode": "double"})
	emitAndValidate(connectorParameters, changeItem, map[string]string{"decimal.handling.mode": "string"})
}
