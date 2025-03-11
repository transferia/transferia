package engine

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util/jsonx"
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

func TestNotElastic(t *testing.T) {
	var canonArr []interface{}
	for _, line := range rawLines {
		if line == "" {
			continue
		}
		parser, err := NewAuditTrailsV1ParserImpl(
			false,
			false,
			logger.Log,
			stats.NewSourceStats(metrics.NewRegistry()),
		)
		require.NoError(t, err)
		msg := makePersqueueReadMessage(0, line)
		result := parser.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: "my-topic-name"})
		require.Len(t, result, 1)
		result[0] = normalizeChangeItem(result[0])
		canonArr = append(canonArr, result[0])
		fmt.Println(result[0].ToJSONString())
		abstract.Dump(result)
	}
	canon.SaveJSON(t, canonArr)
}

func TestElastic(t *testing.T) {
	var canonArr []interface{}
	for _, line := range rawLines {
		if line == "" {
			continue
		}
		parser, err := NewAuditTrailsV1ParserImpl(
			true,
			false,
			logger.Log,
			stats.NewSourceStats(metrics.NewRegistry()),
		)
		require.NoError(t, err)
		msg := makePersqueueReadMessage(0, line)
		result := parser.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: "my-topic-name"})
		require.Len(t, result, 1)
		result[0] = normalizeChangeItem(result[0])
		canonArr = append(canonArr, result[0])
		fmt.Println(result[0].ToJSONString())
		abstract.Dump(result)
	}
	canon.SaveJSON(t, canonArr)
}

func TestMakeHungarianNotationOnSaved(t *testing.T) {
	var canonArr []interface{}
	for _, line := range rawLines {
		if line == "" {
			continue
		}
		result, err := makeHungarianNotation(line)
		require.NoError(t, err)

		q, err := json.Marshal(result)
		require.NoError(t, err)
		fmt.Println(string(q))

		canonArr = append(canonArr, result)
	}
	canon.SaveJSON(t, canonArr)
}

func TestDetermineSuffix(t *testing.T) {
	checkCase := func(t *testing.T, jsonString, expectedSuffix string) {
		var myMap map[string]any
		err := jsonx.NewDefaultDecoder(strings.NewReader(jsonString)).Decode(&myMap)
		require.NoError(t, err)

		suffix := determineSuffix(myMap["k"])
		require.Equal(t, expectedSuffix, suffix)
	}

	checkCase(t, `{"k": 1}`, "num")
	checkCase(t, `{"k": "1"}`, "str")
	checkCase(t, `{"k": true}`, "bool")
	checkCase(t, `{"k": {}}`, "obj")

	checkCase(t, `{"k": [1]}`, "arr__num")
	checkCase(t, `{"k": ["1"]}`, "arr__str")
	checkCase(t, `{"k": [true]}`, "arr__bool")
	checkCase(t, `{"k": [{}]}`, "arr__obj")

	checkCase(t, `{"k": [[1]]}`, "arr__arr__num")
	checkCase(t, `{"k": [["1"]]}`, "arr__arr__str")
	checkCase(t, `{"k": [[true]]}`, "arr__arr__bool")
	checkCase(t, `{"k": [[{}]]}`, "arr__arr__obj")
}
