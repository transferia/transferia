package sink

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
)

func TestS3ObjectRef_s3KeyRegex(t *testing.T) {
	refWithNs := NewS3ObjectRef("", "ns", "tbl", "", "", model.ParsingFormatJSONLine, s3_model.NoEncoding)
	refNoNs := NewS3ObjectRef("", "", "tbl", "", "", model.ParsingFormatJSONLine, s3_model.NoEncoding)

	tests := []struct {
		name    string
		ref     S3ObjectRef
		key     string
		matches bool
	}{
		{"match with layout", refWithNs, "2026/07/02/ns/tbl/part-1751328000000-abcd1234.00001.jsonl", true},
		{"match without layout", refWithNs, "ns/tbl/part-1751328000000-abcd1234.00001.jsonl", true},
		{"match gz suffix", refWithNs, "2026/07/02/ns/tbl/part-1751328000000-abcd1234.00001.jsonl.gz", true},
		{"match zero timestamp", refWithNs, "ns/tbl/part-0-00000000.00000.jsonl", true},
		{"non-match wrong table", refWithNs, "ns/other/part-1-abcd1234.00001.jsonl", false},
		{"non-match wrong namespace", refWithNs, "other/tbl/part-1-abcd1234.00001.jsonl", false},
		{"non-match wrong format", refWithNs, "ns/tbl/part-1-abcd1234.00001.csv", false},
		{"non-match short counter", refWithNs, "ns/tbl/part-1-abcd1234.0001.jsonl", false},
		{"non-match extra segment", refWithNs, "ns/tbl/part-1-abcd1234.00001.jsonl/extra", false},
		{"non-match short hash", refWithNs, "ns/tbl/part-1-abcd123.00001.jsonl", false},
		{"empty-ns matches bare table", refNoNs, "tbl/part-1-abcd1234.00001.jsonl", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			re := tc.ref.partKeyRegex()
			require.Equal(t, tc.matches, re.MatchString(tc.key), "key=%q pattern=%q", tc.key, re.String())
		})
	}
}
