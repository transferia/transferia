package sink

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/transferia/transferia/pkg/abstract/model"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

const partIDHashLen = 8

// S3ObjectRef identifies a logical file stream in S3.
// Files belonging to the same stream share the same namespace, tableName, partID, etc.
// The incrementing counter (managed by FileSplitter) distinguishes individual files within a stream.
type S3ObjectRef struct {
	prefix       string
	namespace    string
	tableName    string
	partID       string
	partIDHash   string
	timestamp    string
	outputFormat model.ParsingFormat
	encoding     s3_v1_model.Encoding
}

// FileStreamKey returns a unique key for FileSplitter maps for this logical stream.
func (b S3ObjectRef) FileStreamKey() string {
	var builder strings.Builder
	builder.Grow(len(b.prefix) + len(b.namespace) + len(b.tableName) + len(b.partID) + len(b.timestamp) + 32)
	builder.WriteString(b.prefix)
	builder.WriteByte(0)
	builder.WriteString(b.namespace)
	builder.WriteByte(0)
	builder.WriteString(b.tableName)
	builder.WriteByte(0)
	builder.WriteString(b.partID)
	builder.WriteByte(0)
	builder.WriteString(b.timestamp)
	builder.WriteByte(0)
	builder.WriteString(string(b.outputFormat))
	builder.WriteByte(0)
	builder.WriteString(string(b.encoding))
	return builder.String()
}

// NewS3ObjectRef builds an S3ObjectRef. layout is an optional path prefix (e.g. worker shard).
// layout, namespace, tableName, partID are trimmed of leading and trailing slashes to duplicate slashes in the key.
func NewS3ObjectRef(
	prefix string,
	namespace string,
	tableName string,
	partID string,
	timestamp string,
	outputFormat model.ParsingFormat,
	encoding s3_v1_model.Encoding,
) S3ObjectRef {
	return S3ObjectRef{
		prefix:       strings.Trim(prefix, "/"),
		namespace:    strings.Trim(namespace, "/"),
		tableName:    strings.Trim(tableName, "/"),
		partID:       strings.Trim(partID, "/"),
		partIDHash:   hashPartID(partID),
		timestamp:    timestamp,
		outputFormat: outputFormat,
		encoding:     encoding,
	}
}

func (b *S3ObjectRef) basePathWithPrefix() string {
	if b.prefix != "" {
		return b.prefix + "/" + b.basePath()
	}
	return b.basePath()
}

// basePath returns the directory prefix: <namespace>/<table_name> or just <table_name>
func (b *S3ObjectRef) basePath() string {
	if b.namespace != "" {
		return b.namespace + "/" + b.tableName
	}
	return b.tableName
}

// PartFileName returns only the data file name segment:
// part-<timestamp>-<hash(partID)>.<NNNNN>.<format>[.gz]
// (no layout and no namespace/table prefix). Used e.g. for Iceberg parquet paths under table location.
func (b *S3ObjectRef) PartFileName(counter int) string {
	return b.partFileSuffix(counter)
}

func (b *S3ObjectRef) partFileSuffix(counter int) string {
	ext := strings.ToLower(string(b.outputFormat))
	counterStr := fmt.Sprintf("%05d", counter)

	var builder strings.Builder
	size := len("part-") + len(b.timestamp) + 1 + partIDHashLen + 1 + len(counterStr) + 1 + len(ext)
	if b.encoding == s3_v1_model.GzipEncoding {
		size += len(".gz")
	}
	builder.Grow(size)
	builder.WriteString("part-")
	builder.WriteString(b.timestamp)
	builder.WriteByte('-')
	builder.WriteString(b.partIDHash)
	builder.WriteByte('.')
	builder.WriteString(counterStr)
	builder.WriteByte('.')
	builder.WriteString(ext)
	if b.encoding == s3_v1_model.GzipEncoding {
		builder.WriteString(".gz")
	}
	return builder.String()
}

// FullKey returns the complete S3 key:
// [<prefix>/]<namespace>/<table_name>/part-<timestamp>-<hash(partID)>.<NNNNN>.<format>[.gz]
// When layout is non-empty it is prepended as a subdirectory prefix.
func (b *S3ObjectRef) FullKey(counter int) string {
	basePath := b.basePath()
	tail := b.partFileSuffix(counter)

	var builder strings.Builder
	size := len(basePath) + 1 + len(tail)
	if b.prefix != "" {
		size += len(b.prefix) + 1
	}
	builder.Grow(size)
	if b.prefix != "" {
		builder.WriteString(b.prefix)
		builder.WriteByte('/')
	}
	builder.WriteString(basePath)
	builder.WriteByte('/')
	builder.WriteString(tail)
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
