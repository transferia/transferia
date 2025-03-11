package common

import "github.com/transferria/transferria/pkg/abstract"

type KafkaTypeDescr struct {
	KafkaTypeAndDebeziumNameAndExtra func(colSchema *abstract.ColSchema, intoArr, isSnapshot bool, connectorParameters map[string]string) (string, string, map[string]interface{})
	// string - type
	// string - name
	// map[string]interface{} - other additional k-v in json field descr - for example: parameters/fields/doc
}
