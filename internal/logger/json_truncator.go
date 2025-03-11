package logger

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/pkg/stringutil"
	"github.com/transferria/transferria/pkg/util/size"
	"go.ytsaurus.tech/library/go/core/log"
)

type JSONTruncatorConfig struct {
	TotalLimit  int
	StringLimit int
	BytesLimit  int
}

type JSONTruncator struct {
	writer                 io.Writer
	logger                 log.Logger
	config                 JSONTruncatorConfig
	fieldTruncatedSizeHist metrics.Histogram
	bytesWritten           metrics.Counter
}

func NewJSONTruncator(
	writer io.Writer,
	logger log.Logger,
	config JSONTruncatorConfig,
	registry metrics.Registry,
) *JSONTruncator {
	return &JSONTruncator{
		writer:                 writer,
		logger:                 logger,
		config:                 config,
		fieldTruncatedSizeHist: registry.Histogram("logger.field_truncated_size_hist", size.DefaultBuckets()),
		bytesWritten:           registry.Counter("logger.bytes_written"),
	}
}

func (w *JSONTruncator) Write(p []byte) (int, error) {
	if w.config.TotalLimit > 0 && len(p) > w.config.TotalLimit {
		p = w.truncateJSON(p)
	}

	n, err := w.writer.Write(p)
	w.bytesWritten.Add(int64(n))

	return n, err
}

func (w *JSONTruncator) truncateJSON(data []byte) []byte {
	var obj map[string]interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		w.logger.Warn("unable to unmarshal json", log.Error(err))
		return data
	}

	truncationInfo := map[string]interface{}{}
	for key, x := range obj {
		switch value := x.(type) {
		case string:
			if w.config.StringLimit > 0 && len(value) > w.config.StringLimit {
				if bytes, err := base64.StdEncoding.DecodeString(value); err == nil {
					if w.config.BytesLimit > 0 && len(bytes) > w.config.BytesLimit {
						truncated := bytes[:w.config.BytesLimit]
						obj[key] = base64.StdEncoding.EncodeToString(truncated)
						w.addTruncationInfo(truncationInfo, key, len(bytes)-len(truncated))
					}
				} else {
					truncated := stringutil.TruncateUTF8(value, w.config.StringLimit)
					obj[key] = truncated
					w.addTruncationInfo(truncationInfo, key, len(value)-len(truncated))
				}
			}
		}
	}

	for key, value := range truncationInfo {
		if _, ok := obj[key]; ok {
			w.logger.Warnf("json already contains key '%v'", key)
			continue
		}
		obj[key] = value
	}

	bytes, err := json.Marshal(obj)
	if err != nil {
		w.logger.Warn("unable to marshal json", log.Error(err))
		return data
	}

	return bytes
}

func (w *JSONTruncator) addTruncationInfo(truncationInfo map[string]interface{}, key string, truncatedSize int) {
	truncationInfo[fmt.Sprintf("%v_truncated_size", key)] = truncatedSize
	w.fieldTruncatedSizeHist.RecordValue(float64(truncatedSize))
}
