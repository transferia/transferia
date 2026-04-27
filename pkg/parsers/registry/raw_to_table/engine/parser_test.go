package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/util/raw_to_table_common"
)

type cfgOpt func(*raw_to_table_common.CommonConfig)

func withTS() cfgOpt {
	return func(c *raw_to_table_common.CommonConfig) { c.IsTimestampEnabled = true }
}

func withHeaders() cfgOpt {
	return func(c *raw_to_table_common.CommonConfig) { c.IsHeadersEnabled = true }
}

func withKey(keyType raw_to_table_common.DataType) cfgOpt {
	return func(c *raw_to_table_common.CommonConfig) {
		c.IsKeyEnabled = true
		c.KeyType = keyType
	}
}

func withDLQSuffix(s string) cfgOpt {
	return func(c *raw_to_table_common.CommonConfig) { c.DLQSuffix = s }
}

// testCfg builds a CommonConfig for tests: default KeyType is Bytes when key column is disabled (empty KeyType).
func testCfg(table string, valueType raw_to_table_common.DataType, opts ...cfgOpt) *raw_to_table_common.CommonConfig {
	c := raw_to_table_common.CommonConfig{TableName: table, ValueType: valueType}
	for _, o := range opts {
		o(&c)
	}
	if !c.IsKeyEnabled && c.KeyType == "" {
		c.KeyType = raw_to_table_common.Bytes
	}
	return &c
}

func rowBase(p abstract.Partition, offset uint32) []any {
	return []any{p.Topic, p.Partition, offset}
}

func rowWithTS(p abstract.Partition, offset uint32, ts time.Time, tail ...any) []any {
	return append(append(rowBase(p, offset), ts), tail...)
}

func rowNoTS(p abstract.Partition, offset uint32, tail ...any) []any {
	return append(rowBase(p, offset), tail...)
}

func newTestMsg(offset int64, wt time.Time, value []byte) parsers.Message {
	return parsers.Message{Offset: uint64(offset), WriteTime: wt, Value: value}
}

func testMsg(offset int64, wt time.Time, value, key []byte, hdr map[string]string) parsers.Message {
	return parsers.Message{Offset: uint64(offset), WriteTime: wt, Value: value, Key: key, Headers: hdr}
}

func dlqRow(p abstract.Partition, msg parsers.Message, failure string) []any {
	return []any{
		p.Topic, p.Partition, uint32(msg.Offset), msg.WriteTime,
		msg.Headers, msg.Key, msg.Value, failure,
	}
}

//-----------------------------------------------------------------------------------------------------

func TestDo_ValidMessages(t *testing.T) {
	tu := time.Unix(0, 123456789)
	p := abstract.Partition{Partition: 1, Topic: "test_topic"}
	headers := map[string]string{"header": "value"}
	keyB, valB := []byte{0x01, 0x02}, []byte{0x03, 0x04}

	for _, tc := range []struct {
		name string
		cfg  *raw_to_table_common.CommonConfig
		msg  parsers.Message
		want []any
	}{
		{
			name: "StringValueNoKey",
			cfg:  testCfg("test_table", raw_to_table_common.String, withTS()),
			msg:  newTestMsg(200, tu, []byte("Hello World!")),
			want: rowWithTS(p, 200, tu, "Hello World!"),
		},
		{
			name: "BytesValueWithHeadersAndKeyString",
			cfg:  testCfg("test_table", raw_to_table_common.Bytes, withTS(), withHeaders(), withKey(raw_to_table_common.String)),
			msg:  testMsg(201, tu, valB, []byte("key"), headers),
			want: rowWithTS(p, 201, tu, headers, "key", valB),
		},
		{
			name: "BytesValueWithKeyBytes",
			cfg:  testCfg("test_table", raw_to_table_common.Bytes, withTS(), withKey(raw_to_table_common.Bytes)),
			msg:  testMsg(202, tu, valB, keyB, nil),
			want: rowWithTS(p, 202, tu, keyB, valB),
		},
		{
			name: "NilKVBytes",
			cfg:  testCfg("test_table", raw_to_table_common.Bytes, withKey(raw_to_table_common.Bytes)),
			msg:  testMsg(202, tu, nil, nil, nil),
			want: rowNoTS(p, 202, nil, nil),
		},
		{
			name: "NilKVStrings",
			cfg:  testCfg("test_table", raw_to_table_common.String, withKey(raw_to_table_common.String)),
			msg:  testMsg(202, tu, nil, nil, nil),
			want: rowNoTS(p, 202, nil, nil),
		},
		{
			name: "NilKVJSONs",
			cfg:  testCfg("test_table", raw_to_table_common.JSON, withKey(raw_to_table_common.JSON)),
			msg:  testMsg(202, tu, nil, nil, nil),
			want: rowNoTS(p, 202, nil, nil),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ptr, err := NewRawToTable(tc.cfg)
			require.NoError(t, err)
			res := ptr.Do(tc.msg, p)
			require.Len(t, res, 1)
			require.Equal(t, tc.want, res[0].ColumnValues)
			require.Equal(t, tc.cfg.TableName, res[0].Table)
		})
	}
}

func TestDoBatch_EquivalentToDo(t *testing.T) {
	tu := time.Unix(0, 555666777)
	p := abstract.Partition{Partition: 7, Topic: "batch_topic"}
	cfg := testCfg("batch_table", raw_to_table_common.String, withTS())
	ptr, err := NewRawToTable(cfg)
	require.NoError(t, err)

	msgs := []parsers.Message{
		newTestMsg(10, tu, []byte("a")),
		newTestMsg(11, tu, []byte("b")),
	}
	got := ptr.DoBatch(parsers.MessageBatch{Partition: p.Partition, Topic: p.Topic, Messages: msgs})
	require.Len(t, got, len(msgs))
	for i := range msgs {
		want := ptr.Do(msgs[i], p)
		require.Len(t, want, 1)
		require.Equal(t, want[0], got[i], "message index %d", i)
	}
}

func TestDo_JSONKeyWithBytesValue(t *testing.T) {
	tu := time.Unix(0, 424242424)
	p := abstract.Partition{Partition: 3, Topic: "json_key_topic"}
	valB := []byte{0xAB, 0xCD}
	cfg := testCfg("t_json_key", raw_to_table_common.Bytes, withTS(), withKey(raw_to_table_common.JSON))
	ptr, err := NewRawToTable(cfg)
	require.NoError(t, err)
	res := ptr.Do(testMsg(500, tu, valB, []byte(`{"id":42,"name":"x"}`), nil), p)
	require.Len(t, res, 1)
	require.Equal(t, "t_json_key", res[0].Table)
	require.Equal(t, rowWithTS(p, 500, tu, map[string]any{
		"id": json.Number("42"), "name": "x",
	}, valB), res[0].ColumnValues)
}

func TestDo_JSONValue_EmptyObjectAndEmptyArray(t *testing.T) {
	tu := time.Unix(0, 333222111)
	p := abstract.Partition{Partition: 4, Topic: "edge_json_topic"}
	cfg := testCfg("t_edge", raw_to_table_common.JSON)
	ptr, err := NewRawToTable(cfg)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		msg  parsers.Message
		want []any
	}{
		{"empty_object", newTestMsg(1, tu, []byte(`{}`)), rowNoTS(p, 1, map[string]any{})},
		{"empty_array", newTestMsg(2, tu, []byte(`[]`)), rowNoTS(p, 2, []any{})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ptr.Do(tc.msg, p)
			require.Len(t, res, 1)
			require.Equal(t, tc.want, res[0].ColumnValues)
		})
	}
}

func TestDo_ValidJSONValue(t *testing.T) {
	tu := time.Unix(0, 111222333)
	p := abstract.Partition{Partition: 2, Topic: "json_topic"}
	cfg := testCfg("t_json", raw_to_table_common.JSON, withTS())
	ptr, err := NewRawToTable(cfg)
	require.NoError(t, err)
	res := ptr.Do(newTestMsg(400, tu, []byte(`{"hello":"world","count":1}`)), p)
	require.Len(t, res, 1)
	require.Equal(t, "t_json", res[0].Table)
	// jsonx.Unmarshal uses json.Number for JSON numbers when decoding into map[string]any.
	require.Equal(t, rowWithTS(p, 400, tu, map[string]any{
		"hello": "world", "count": json.Number("1"),
	}), res[0].ColumnValues)
}

func TestDo_SendsToDLQ(t *testing.T) {
	tu := time.Unix(0, 987654321)
	p := abstract.Partition{Partition: 1, Topic: "test_topic"}
	badUTF := []byte{0xC0, 0xC1, 0xF5}

	for _, tc := range []struct {
		name            string
		cfg             *raw_to_table_common.CommonConfig
		msg             parsers.Message
		expectedTable   string
		expectedFailure string
	}{
		{
			name:            "InvalidStringValue",
			cfg:             testCfg("base", raw_to_table_common.String, withTS()),
			msg:             testMsg(300, tu, badUTF, []byte("key"), nil),
			expectedTable:   "base_dlq",
			expectedFailure: "value is configured as string, but it isn't valid UTF-8",
		},
		{
			name:            "DLQSuffixWithoutLeadingUnderscoreIsNormalized",
			cfg:             testCfg("base", raw_to_table_common.String, withTS(), withDLQSuffix("errors")),
			msg:             testMsg(303, tu, badUTF, []byte("k"), nil),
			expectedTable:   "baseerrors",
			expectedFailure: "value is configured as string, but it isn't valid UTF-8",
		},
		{
			name:            "InvalidStringKey",
			cfg:             testCfg("base", raw_to_table_common.Bytes, withTS(), withKey(raw_to_table_common.String), withDLQSuffix("_errors")),
			msg:             testMsg(301, tu, []byte("ok"), badUTF, nil),
			expectedTable:   "base_errors",
			expectedFailure: "key is configured as string, but it isn't valid UTF-8",
		},
		{
			name:            "InvalidJSONValueUsesTopicFallback",
			cfg:             testCfg("", raw_to_table_common.JSON, withTS()),
			msg:             testMsg(302, tu, []byte(`{"foo":`), []byte("key"), map[string]string{"h": "v"}),
			expectedTable:   "test_topic_dlq",
			expectedFailure: "value is configured as JSON, but it isn't valid JSON",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ptr, err := NewRawToTable(tc.cfg)
			require.NoError(t, err)
			res := ptr.Do(tc.msg, p)
			require.Len(t, res, 1)
			require.Equal(t, tc.expectedTable, res[0].Table)
			require.Equal(t, dlqRow(p, tc.msg, tc.expectedFailure), res[0].ColumnValues)
		})
	}
}
