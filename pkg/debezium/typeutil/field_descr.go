package typeutil

import (
	"github.com/transferia/transferia/pkg/abstract"
)

func TimestampPgParamsTypeToKafkaType(colSchema *abstract.ColSchema, intoArr, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	divider, _ := GetTimeDivider(OriginalTypeWithoutProvider(colSchema.OriginalType))
	if intoArr {
		divider = 1
	}
	if divider == 1 {
		return "int64", "io.debezium.time.MicroTimestamp", nil
	} else {
		return "int64", "io.debezium.time.Timestamp", nil
	}
}

func TimestampMysqlParamsTypeToKafkaType(colSchema *abstract.ColSchema, intoArr, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	divider, _ := GetTimeDivider(colSchema.OriginalType)
	if intoArr {
		divider = 1
	}
	if divider == 1 {
		return "int64", "io.debezium.time.MicroTimestamp", nil
	} else {
		return "int64", "io.debezium.time.Timestamp", nil
	}
}

func TimePgWithoutTZParamsToKafkaType(colSchema *abstract.ColSchema, intoArr, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	divider, _ := GetTimeDivider(OriginalTypeWithoutProvider(colSchema.OriginalType))
	if intoArr {
		divider = 1
	}
	if divider == 1 {
		return "int64", "io.debezium.time.MicroTime", nil
	} else {
		return "int32", "io.debezium.time.Time", nil
	}
}

func BitParametersExtractor(colSchema *abstract.ColSchema, _, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	if colSchema.OriginalType == "mysql:bit(1)" {
		return "boolean", "", nil
	} else {
		length, err := GetBitLength(colSchema.OriginalType)
		if err != nil {
			panic("impossible")
		}
		result := map[string]interface{}{
			"parameters": map[string]interface{}{
				"length": length,
			},
		}
		return "bytes", "io.debezium.data.Bits", result
	}
}
