package logger

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/stringutil"
)

type LogRecord struct {
	String              string `json:"string"`
	StringTruncatedSize *int   `json:"string_truncated_size,omitempty"`
	Bytes               string `json:"bytes"`
	BytesTruncatedSize  *int   `json:"bytes_truncated_size,omitempty"`
}

func TestJSONTruncator(t *testing.T) {
	config := JSONTruncatorConfig{
		TotalLimit:  1,
		StringLimit: 7,
		BytesLimit:  7,
	}
	writer := newFakeWriter(t, true, nil)
	truncator := NewJSONTruncator(writer, Log, config, solomon.NewRegistry(solomon.NewRegistryOpts()))

	t.Run("hello 👀 world", func(t *testing.T) {
		testWrite(t, config, writer, truncator, "hello 👀 world")
	})
	t.Run("hello", func(t *testing.T) {
		testWrite(t, config, writer, truncator, "hello")
	})
}

func testWrite(t *testing.T, config JSONTruncatorConfig, writer *fakeWriter, truncator io.Writer, value string) {
	writer.data = nil

	logRecord := LogRecord{
		String: value,
		Bytes:  base64.StdEncoding.EncodeToString([]byte(value)),
	}
	bytes, err := json.Marshal(logRecord)
	require.NoError(t, err)
	_, err = truncator.Write(bytes)
	require.NoError(t, err)
	require.Equal(t, 1, len(writer.data))

	logRecord = LogRecord{}
	require.NoError(t, json.Unmarshal(writer.data[0], &logRecord))
	{
		truncated := value
		if len(value) > config.StringLimit {
			truncated = stringutil.TruncateUTF8(value, config.StringLimit)
			require.NotNil(t, logRecord.StringTruncatedSize)
			require.Equal(t, len(value)-len(truncated), *logRecord.StringTruncatedSize)
		} else {
			require.Nil(t, logRecord.StringTruncatedSize)
		}
		require.Equal(t, truncated, logRecord.String)
	}
	{
		truncated := []byte(value)
		if len(value) > config.BytesLimit {
			truncated = []byte(value[:config.BytesLimit])
			require.NotNil(t, logRecord.BytesTruncatedSize)
			require.Equal(t, len(value)-len(truncated), *logRecord.BytesTruncatedSize)
		} else {
			require.Nil(t, logRecord.BytesTruncatedSize)
		}
		require.Equal(t, base64.StdEncoding.EncodeToString(truncated), logRecord.Bytes)
	}
}
