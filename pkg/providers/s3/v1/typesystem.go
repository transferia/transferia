package s3v1

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const ProviderType = s3_v1_model.ProviderType

func init() {
	typesystem.TargetRule(ProviderType, map[ytschema.Type]string{
		ytschema.TypeInt64:     "",
		ytschema.TypeInt32:     "",
		ytschema.TypeInt16:     "",
		ytschema.TypeInt8:      "",
		ytschema.TypeUint64:    "",
		ytschema.TypeUint32:    "",
		ytschema.TypeUint16:    "",
		ytschema.TypeUint8:     "",
		ytschema.TypeFloat32:   "",
		ytschema.TypeFloat64:   "",
		ytschema.TypeBytes:     "",
		ytschema.TypeString:    "",
		ytschema.TypeBoolean:   "",
		ytschema.TypeAny:       "",
		ytschema.TypeDate:      "",
		ytschema.TypeDatetime:  "",
		ytschema.TypeTimestamp: "",
	})
}
