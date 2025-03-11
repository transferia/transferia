package engine

import (
	_ "embed"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/internal/metrics"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/parsers"
	"github.com/transferria/transferria/pkg/stats"
	"golang.org/x/exp/maps"
)

var rawLines []string

//go:embed parser_test.jsonl
var parserTest []byte

func init() {
	rawLines = strings.Split(string(parserTest), "\n")
}

func makePersqueueReadMessage(i int, rawLine string) parsers.Message {
	return parsers.Message{
		Offset:     uint64(i),
		SeqNo:      0,
		Key:        []byte("test_source_id"),
		CreateTime: time.Now(),
		WriteTime:  time.Now(),
		Value:      []byte(rawLine),
		Headers:    map[string]string{"some_field": "test"},
	}
}

func normalizeChangeItem(in abstract.ChangeItem) abstract.ChangeItem {
	m := in.AsMap()
	keys := maps.Keys(m)
	sort.Strings(keys)
	for i, key := range keys {
		in.ColumnNames[i] = key
		in.ColumnValues[i] = m[key]

		if key == "_timestamp" {
			in.ColumnValues[i] = "2022-12-15T22:13:38.403419294+03:00"
		}
	}
	in.CommitTime = 0
	return in
}

func TestParser(t *testing.T) {
	var canonArr []interface{}
	for _, line := range rawLines {
		if line == "" {
			continue
		}
		parser := NewCloudLoggingImpl(false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
		msg := makePersqueueReadMessage(0, line)
		result := parser.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: "my-topic-name"})
		require.Len(t, result, 1)
		require.Equal(t, "2022-11-18 05:39:48.017249864 +0000 UTC", result[0].ColumnValues[0].(time.Time).String())
		result[0] = normalizeChangeItem(result[0])
		canonArr = append(canonArr, result[0])
		fmt.Println(result[0].ToJSONString())
		abstract.Dump(result)
	}
	canon.SaveJSON(t, canonArr)
}
