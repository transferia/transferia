package sink

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/transferia/transferia/pkg/abstract/model"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
)

const partIDHashLen = 8

// s3ObjectRef identifies a logical file stream in S3.
// Files belonging to the same stream share the same namespace, tableName, partID, etc.
// The incrementing counter (managed by FileSplitter) distinguishes individual files within a stream.
type s3ObjectRef struct {
	layout       string
	namespace    string
	tableName    string
	partID       string
	partIDHash   string
	timestamp    string
	outputFormat model.ParsingFormat
	encoding     s3_provider.Encoding
}

func newS3ObjectRef(
	layout string,
	namespace string,
	tableName string,
	partID string,
	timestamp string,
	outputFormat model.ParsingFormat,
	encoding s3_provider.Encoding,
) s3ObjectRef {
	return s3ObjectRef{
		layout:       layout,
		namespace:    namespace,
		tableName:    tableName,
		partID:       partID,
		partIDHash:   hashPartID(partID),
		timestamp:    timestamp,
		outputFormat: outputFormat,
		encoding:     encoding,
	}
}

// basePath returns the directory prefix: <namespace>/<table_name> or just <table_name>
func (b *s3ObjectRef) basePath() string {
	if b.namespace != "" {
		return b.namespace + "/" + b.tableName
	}
	return b.tableName
}

// fullKey returns the complete S3 key:
// [<layout>/]<namespace>/<table_name>/part-<timestamp>-<hash(partID)>.<NNNNN>.<format>[.gz]
// When layout is non-empty it is prepended as a subdirectory prefix.
func (b *s3ObjectRef) fullKey(counter int) string {
	basePath := b.basePath()
	ext := strings.ToLower(string(b.outputFormat))
	counterStr := fmt.Sprintf("%05d", counter)

	var builder strings.Builder
	size := len(basePath) + len("/part-") + len(b.timestamp) + len("-") + partIDHashLen + len(".") + len(counterStr) + len(".") + len(ext)
	if b.layout != "" {
		size += len(b.layout) + len("/")
	}
	if b.encoding == s3_provider.GzipEncoding {
		size += len(".gz")
	}
	builder.Grow(size)
	if b.layout != "" {
		builder.WriteString(b.layout)
		builder.WriteByte('/')
	}
	builder.WriteString(basePath)
	builder.WriteByte('/')
	builder.WriteString("part-")
	builder.WriteString(b.timestamp)
	builder.WriteByte('-')
	builder.WriteString(b.partIDHash)
	builder.WriteByte('.')
	builder.WriteString(counterStr)
	builder.WriteByte('.')
	builder.WriteString(ext)
	if b.encoding == s3_provider.GzipEncoding {
		builder.WriteString(".gz")
	}
	return builder.String()
}

// hashPartID produces a short MD5 hash of the partID, trimmed to partIDHashLen characters.
// If partID is empty, "default" is used as input to produce a deterministic hash.
func hashPartID(partID string) string {
	if partID == "" {
		partID = "default"
	}
	h := md5.Sum([]byte(partID))
	return hex.EncodeToString(h[:])[:partIDHashLen]
}
