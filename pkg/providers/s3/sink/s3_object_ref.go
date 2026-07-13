package sink

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
)

const partIDHashLen = 8

// S3ObjectRef identifies a logical file stream in S3.
// Files belonging to the same stream share the same namespace, tableName, partID, etc.
// The incrementing counter (managed by FileSplitter) distinguishes individual files within a stream.
type S3ObjectRef struct {
	layout       string
	namespace    string
	tableName    string
	partID       string
	partIDHash   string
	timestamp    string
	outputFormat model.ParsingFormat
	encoding     s3_model.Encoding
}

// FileStreamKey returns a unique key for FileSplitter maps for this logical stream.
func (b S3ObjectRef) FileStreamKey() string {
	var builder strings.Builder
	builder.Grow(len(b.layout) + len(b.namespace) + len(b.tableName) + len(b.partID) + len(b.timestamp) + 32)
	builder.WriteString(b.layout)
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
	layout string,
	namespace string,
	tableName string,
	partID string,
	timestamp string,
	outputFormat model.ParsingFormat,
	encoding s3_model.Encoding,
) S3ObjectRef {
	return S3ObjectRef{
		layout:       strings.Trim(layout, "/"),
		namespace:    strings.Trim(namespace, "/"),
		tableName:    strings.Trim(tableName, "/"),
		partID:       strings.Trim(partID, "/"),
		partIDHash:   hashPartID(partID),
		timestamp:    timestamp,
		outputFormat: outputFormat,
		encoding:     encoding,
	}
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
	if b.encoding == s3_model.GzipEncoding {
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
	if b.encoding == s3_model.GzipEncoding {
		builder.WriteString(".gz")
	}
	return builder.String()
}

// FullKey returns the complete S3 key:
// [<layout>/]<namespace>/<table_name>/part-<timestamp>-<hash(partID)>.<NNNNN>.<format>[.gz]
// When layout is non-empty it is prepended as a subdirectory prefix.
func (b *S3ObjectRef) FullKey(counter int) string {
	basePath := b.basePath()
	tail := b.partFileSuffix(counter)

	var builder strings.Builder
	size := len(basePath) + 1 + len(tail)
	if b.layout != "" {
		size += len(b.layout) + 1
	}
	builder.Grow(size)
	if b.layout != "" {
		builder.WriteString(b.layout)
		builder.WriteByte('/')
	}
	builder.WriteString(basePath)
	builder.WriteByte('/')
	builder.WriteString(tail)
	return builder.String()
}

// partKeyRegex matches every part-file key this sink may write for the table (any layout prefix,
// timestamp, partID hash, or file counter). Used to filter list-by-prefix results during cleanup.
func (b *S3ObjectRef) partKeyRegex() *regexp.Regexp {
	tbl := regexp.QuoteMeta(b.tableName)
	fmtExt := regexp.QuoteMeta(strings.ToLower(string(b.outputFormat)))

	var pattern string
	if b.namespace != "" {
		ns := regexp.QuoteMeta(b.namespace)
		pattern = `^(?:.*/)?` + ns + `/` + tbl + `/part-\d+-[a-fA-F0-9]{8}\.\d{5}\.` + fmtExt + `(?:\.gz)?$`
	} else {
		pattern = `^(?:.*/)?` + tbl + `/part-\d+-[a-fA-F0-9]{8}\.\d{5}\.` + fmtExt + `(?:\.gz)?$`
	}
	return regexp.MustCompile(pattern)
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
