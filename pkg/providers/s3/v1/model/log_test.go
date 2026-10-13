package s3_model

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.uber.org/zap/zapcore"
)

func TestDestinationLogHidesEncryptionKey(t *testing.T) {
	key := strings.Repeat("0f", 32)
	dst := &S3Destination{Bucket: "bucket", EncryptionKey: model.SecretString(key)}

	enc := zapcore.NewMapObjectEncoder()
	require.NoError(t, dst.MarshalLogObject(enc))
	require.Equal(t, "***HIDDEN***", enc.Fields["EncryptionKey"])
	require.NotContains(t, fmt.Sprint(enc.Fields), key)
}
