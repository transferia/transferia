package debezium

import (
	"testing"

	"github.com/stretchr/testify/require"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestReceiveFieldErrorInsteadOfPanic(t *testing.T) {
	inSchemaDescr := &debeziumcommon.Schema{
		Type: ytschema.TypeBytes.String(),
	}
	var val interface{} = map[string]interface{}{
		"k": 1,
	}
	originalType := &debeziumcommon.OriginalTypeInfo{
		OriginalType: "pg:bytea",
	}
	_, isAbsent, err := receiveField(inSchemaDescr, val, originalType, false)
	require.Error(t, err)
	require.False(t, isAbsent)
}
