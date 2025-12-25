package abstract

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/schema"
)

func GetTypeFromString(dataType string) (schema.Type, error) {
	switch dataType {
	// int types
	case string(schema.TypeInt8):
		return schema.TypeInt8, nil
	case string(schema.TypeInt16):
		return schema.TypeInt16, nil
	case string(schema.TypeInt32):
		return schema.TypeInt32, nil
	case string(schema.TypeInt64):
		return schema.TypeInt64, nil

	// uint Types
	case string(schema.TypeUint8):
		return schema.TypeUint8, nil
	case string(schema.TypeUint16):
		return schema.TypeUint16, nil
	case string(schema.TypeUint32):
		return schema.TypeUint32, nil
	case string(schema.TypeUint64):
		return schema.TypeUint64, nil

	// float types
	case string(schema.TypeFloat32):
		return schema.TypeFloat32, nil
	case string(schema.TypeFloat64):
		return schema.TypeFloat64, nil

	// textual data types
	case string(schema.TypeString):
		return schema.TypeString, nil
	case string(schema.TypeBytes):
		return schema.TypeBytes, nil

	// date data types
	case string(schema.TypeDate):
		return schema.TypeDate, nil
	case string(schema.TypeDatetime):
		return schema.TypeDatetime, nil
	case string(schema.TypeTimestamp):
		return schema.TypeTimestamp, nil
	case string(schema.TypeInterval):
		return schema.TypeInterval, nil

	// bool data type
	case string(schema.TypeBoolean):
		return schema.TypeBoolean, nil

	// any data type
	case string(schema.TypeAny):
		return schema.TypeAny, nil

	default:
		return "", NewFatalError(xerrors.Errorf("unknown data type provided %s", dataType))
	}
}
