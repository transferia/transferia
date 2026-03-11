package reader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestTokenizeFormatSimple(t *testing.T) {
	tokens := tokenizeFormat(`$remote_addr $status`)
	require.Len(t, tokens, 3)
	require.Equal(t, nginxToken{IsVariable: true, Value: "remote_addr"}, tokens[0])
	require.Equal(t, nginxToken{IsVariable: false, Value: " "}, tokens[1])
	require.Equal(t, nginxToken{IsVariable: true, Value: "status"}, tokens[2])
}

func TestTokenizeFormatQuoted(t *testing.T) {
	tokens := tokenizeFormat(`"$remote_addr" "-" "$status"`)
	require.Len(t, tokens, 5)
	require.Equal(t, nginxToken{IsVariable: false, Value: `"`}, tokens[0])
	require.Equal(t, nginxToken{IsVariable: true, Value: "remote_addr"}, tokens[1])
	require.Equal(t, nginxToken{IsVariable: false, Value: `" "-" "`}, tokens[2])
	require.Equal(t, nginxToken{IsVariable: true, Value: "status"}, tokens[3])
	require.Equal(t, nginxToken{IsVariable: false, Value: `"`}, tokens[4])
}

func TestCompileFormatQuoted(t *testing.T) {
	compiled, err := compileFormat(`"$remote_addr" "-" "$status"`)
	require.NoError(t, err)
	require.Len(t, compiled.fields, 2)
	require.Equal(t, "remote_addr", compiled.fields[0].Name)
	require.Equal(t, "status", compiled.fields[1].Name)

	values, _, err := compiled.parseEntry(`"212.28.183.93" "-" "403"`)
	require.NoError(t, err)
	require.Equal(t, []string{"212.28.183.93", "403"}, values)
}

func TestCompileFormatWithBrackets(t *testing.T) {
	compiled, err := compileFormat(`$remote_addr - $remote_user [$time_local] "$request" $status`)
	require.NoError(t, err)
	require.Len(t, compiled.fields, 5)

	values, _, err := compiled.parseEntry(`192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.1" 200`)
	require.NoError(t, err)
	require.Equal(t, "192.168.1.1", values[0])
	require.Equal(t, "frank", values[1])
	require.Equal(t, "10/Oct/2000:13:55:36 -0700", values[2])
	require.Equal(t, "GET /index.html HTTP/1.1", values[3])
	require.Equal(t, "200", values[4])
}

func TestCompileFormatSchemaTypes(t *testing.T) {
	compiled, err := compileFormat(`"$status" "$request_time" "[$time_local]" "$host"`)
	require.NoError(t, err)
	require.Len(t, compiled.schema, 4)

	require.Equal(t, "status", compiled.schema[0].ColumnName)
	require.Equal(t, schema.TypeInt64.String(), compiled.schema[0].DataType)
	require.Equal(t, "0", compiled.schema[0].Path)

	require.Equal(t, "request_time", compiled.schema[1].ColumnName)
	require.Equal(t, schema.TypeFloat64.String(), compiled.schema[1].DataType)
	require.Equal(t, "1", compiled.schema[1].Path)

	require.Equal(t, "time_local", compiled.schema[2].ColumnName)
	require.Equal(t, schema.TypeDatetime.String(), compiled.schema[2].DataType)
	require.Equal(t, "2", compiled.schema[2].Path)

	require.Equal(t, "host", compiled.schema[3].ColumnName)
	require.Equal(t, schema.TypeString.String(), compiled.schema[3].DataType)
	require.Equal(t, "3", compiled.schema[3].Path)
}

func TestCompileFormatDuplicateNames(t *testing.T) {
	compiled, err := compileFormat(`"$host" "$host"`)
	require.NoError(t, err)
	require.Len(t, compiled.schema, 2)
	require.Equal(t, "host", compiled.schema[0].ColumnName)
	require.Equal(t, "host_2", compiled.schema[1].ColumnName)
}

func TestCompileFormatEmpty(t *testing.T) {
	_, err := compileFormat("")
	require.Error(t, err)
}

func TestCompileFormatNoVariables(t *testing.T) {
	_, err := compileFormat(`"static" "text"`)
	require.Error(t, err)
}

func TestParseEntryMismatch(t *testing.T) {
	compiled, err := compileFormat(`"$remote_addr" "$status"`)
	require.NoError(t, err)
	_, _, err = compiled.parseEntry(`this does not match`)
	require.Error(t, err)
}

func TestCompileFormatPipeSeparated(t *testing.T) {
	compiled, err := compileFormat(`$remote_addr|$status|$request_time`)
	require.NoError(t, err)
	values, _, err := compiled.parseEntry(`192.168.1.1|200|0.042`)
	require.NoError(t, err)
	require.Equal(t, []string{"192.168.1.1", "200", "0.042"}, values)
}

func TestCompileFormatTabSeparated(t *testing.T) {
	compiled, err := compileFormat("$remote_addr\t$status\t$host")
	require.NoError(t, err)
	values, _, err := compiled.parseEntry("192.168.1.1\t200\texample.com")
	require.NoError(t, err)
	require.Equal(t, []string{"192.168.1.1", "200", "example.com"}, values)
}

func TestParseEntryFullCDNFormat(t *testing.T) {
	format := `"$remote_addr" "-" "$remote_user" "[$time_local]" "$request" "$status" "$body_bytes_sent" "$http_referer" "$http_user_agent" "$bytes_sent" "[$edgename]" "$scheme" "$host" "$request_time" "$upstream_response_time" "$request_length" "$http_range" "[$responding_node]" "$upstream_cache_status" "$upstream_response_length" "$upstream_addr" "$gcdn_api_client_id" "$gcdn_api_resource_id" "$uid_got" "$uid_set" "$geoip_country_code" "$geoip_city" "$shield_type" "$server_addr" "$server_port" "$upstream_status" "-" "$upstream_connect_time" "$upstream_header_time" "$shard_addr" "$geoip2_data_asnumber" "$connection" "$connection_requests" "$request_id" "$http_x_forwarded_proto" "$http_x_forwarded_request_id" "$ssl_cipher" "$ssl_session_id" "$ssl_session_reused" "$sent_http_content_type" "$real_tcpinfo_rtt" "$http_x_forwarded_http_ver" "$vp_enabled" "$geoip2_region"`

	compiled, err := compileFormat(format)
	require.NoError(t, err)
	require.Len(t, compiled.fields, 47)

	line := `"212.28.183.93" "-" "-" "[28/Nov/2025:10:04:24 +0000]" "GET /wp-content/mysql.sql HTTP/1.1" "403" "209" "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36" "483" "[blt-up-gc15]" "http" "linkpayment-test.tbank.ru_cache_sharded" "0.085" "0.074" "687" "-" "[blt]" "MISS" "240" "178.130.128.60:443" "875" "5374336" "-" "-" "US" "St Louis" "shield_no" "193.17.93.194" "10080" "403" "-" "0.047" "0.073" "5.188.7.13" "40021" "3210962417" "5" "fd9206997a94e9d81ddb37515c09199c" "https" "8b37970ca1ef7398ae68e6113969633d" "-" "-" "-" "application/xml" "132957" "HTTP/1.1" "0" "MO"`

	values, consumed, err := compiled.parseEntry(line)
	require.NoError(t, err)
	require.Equal(t, len(line), consumed)
	require.Len(t, values, 47)

	require.Equal(t, "212.28.183.93", values[0])                      // remote_addr
	require.Equal(t, "-", values[1])                                  // remote_user
	require.Equal(t, "28/Nov/2025:10:04:24 +0000", values[2])         // time_local (no brackets!)
	require.Equal(t, "GET /wp-content/mysql.sql HTTP/1.1", values[3]) // request
	require.Equal(t, "403", values[4])                                // status
	require.Equal(t, "209", values[5])                                // body_bytes_sent
	require.Equal(t, "blt-up-gc15", values[9])                        // edgename (no brackets!)
	require.Equal(t, "http", values[10])                              // scheme
	require.Equal(t, "blt", values[16])                               // responding_node (no brackets!)
	require.Equal(t, "MO", values[46])                                // geoip2_region (last field)
}

func TestParseEntryMultiline(t *testing.T) {
	format := `"$remote_addr" "$status"
"$host"`

	compiled, err := compileFormat(format)
	require.NoError(t, err)
	require.Len(t, compiled.fields, 3)

	// Single-line input (with " " instead of format's "\n").
	values, _, err := compiled.parseEntry(`"1.2.3.4" "200" "example.com"`)
	require.NoError(t, err)
	require.Equal(t, []string{"1.2.3.4", "200", "example.com"}, values)

	// Normal multi-line input.
	values, _, err = compiled.parseEntry(`"1.2.3.4" "200"` + "\n" + `"example.com"`)
	require.NoError(t, err)
	require.Equal(t, []string{"1.2.3.4", "200", "example.com"}, values)
}

func TestParseEntryConsumedBytes(t *testing.T) {
	compiled, err := compileFormat(`"$addr" "$status"`)
	require.NoError(t, err)

	input := `"1.2.3.4" "200"` + "\n" + `"5.6.7.8" "404"`
	values, consumed, err := compiled.parseEntry(input)
	require.NoError(t, err)
	require.Equal(t, []string{"1.2.3.4", "200"}, values)
	require.Equal(t, len(`"1.2.3.4" "200"`), consumed)
}

func TestConvertNginxValueDash(t *testing.T) {
	// Dash ("-") means "no value" in nginx logs and should always return nil regardless of the column type.
	for _, dt := range []schema.Type{schema.TypeInt64, schema.TypeFloat64, schema.TypeString, schema.TypeDatetime} {
		col := abstract.NewColSchema("test_col", dt, false)
		result, err := convertNginxValue("-", col)
		require.NoError(t, err, dt.String())
		require.Nil(t, result, dt.String())
	}
}

func TestConvertNginxValueNonDash(t *testing.T) {
	col := abstract.NewColSchema("status", schema.TypeInt64, false)
	result, err := convertNginxValue("200", col)
	require.NoError(t, err)
	require.Equal(t, "200", result)

	col = abstract.NewColSchema("request_time", schema.TypeFloat64, false)
	result, err = convertNginxValue("0.042", col)
	require.NoError(t, err)
	require.Equal(t, "0.042", result)
}

func TestConvertNginxValueDatetime(t *testing.T) {
	col := abstract.NewColSchema("time_local", schema.TypeDatetime, false)

	// Valid datetime.
	result, err := convertNginxValue("28/Nov/2025:10:04:24 +0000", col)
	require.NoError(t, err)
	expected, _ := time.Parse(timeLocalLayout, "28/Nov/2025:10:04:24 +0000")
	require.Equal(t, expected, result)

	// Invalid datetime returns error.
	_, err = convertNginxValue("not-a-date", col)
	require.Error(t, err)

	// Dash returns nil (missing value).
	result, err = convertNginxValue("-", col)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestParseMultipleEntriesSequentially(t *testing.T) {
	compiled, err := compileFormat(`"$addr" "$status"`)
	require.NoError(t, err)
	input := `"1.2.3.4" "200"` + "\n" + `"5.6.7.8" "404"` + "\n" + `"9.0.0.1" "500"`
	pos := 0
	var allValues [][]string
	for pos < len(input) {
		values, consumed, err := compiled.parseEntry(input[pos:])
		require.NoError(t, err)
		allValues = append(allValues, values)
		pos += consumed
		for pos < len(input) && isWhitespace(input[pos]) {
			pos++
		}
	}
	require.Len(t, allValues, 3)
	require.Equal(t, []string{"1.2.3.4", "200"}, allValues[0])
	require.Equal(t, []string{"5.6.7.8", "404"}, allValues[1])
	require.Equal(t, []string{"9.0.0.1", "500"}, allValues[2])
}

func TestParseEntryIncompleteInput(t *testing.T) {
	compiled, err := compileFormat(`"$addr" "$status" "$host"`)
	require.NoError(t, err)
	_, _, err = compiled.parseEntry(`"1.2.3.4" "200`)
	require.Error(t, err)
}

func TestParseEntryDashValuesInCDNFormat(t *testing.T) {
	format := `"$remote_addr" "-" "$remote_user" "[$time_local]" "$request" "$status" "$body_bytes_sent" "$http_referer" "$http_user_agent" "$bytes_sent" "[$edgename]" "$scheme" "$host" "$request_time" "$upstream_response_time" "$request_length" "$http_range" "[$responding_node]" "$upstream_cache_status" "$upstream_response_length" "$upstream_addr" "$gcdn_api_client_id" "$gcdn_api_resource_id" "$uid_got" "$uid_set" "$geoip_country_code" "$geoip_city" "$shield_type" "$server_addr" "$server_port" "$upstream_status" "-" "$upstream_connect_time" "$upstream_header_time" "$shard_addr" "$geoip2_data_asnumber" "$connection" "$connection_requests" "$request_id" "$http_x_forwarded_proto" "$http_x_forwarded_request_id" "$ssl_cipher" "$ssl_session_id" "$ssl_session_reused" "$sent_http_content_type" "$real_tcpinfo_rtt" "$http_x_forwarded_http_ver" "$vp_enabled" "$geoip2_region"`

	compiled, err := compileFormat(format)
	require.NoError(t, err)

	line := `"10.0.0.1" "-" "-" "[28/Nov/2025:10:05:00 +0000]" "GET /health HTTP/1.1" "502" "0" "-" "curl/7.68.0" "256" "[edge-node]" "https" "api.example.com" "0.001" "-" "45" "-" "[origin]" "-" "-" "-" "100" "200" "-" "-" "RU" "Moscow" "shield_no" "10.0.0.1" "443" "-" "-" "-" "-" "10.0.0.2" "12345" "999999" "1" "abc123" "https" "def456" "TLS_AES_128_GCM_SHA256" "-" "-" "text/plain" "5000" "HTTP/2.0" "0" "MOW"`

	values, consumed, err := compiled.parseEntry(line)
	require.NoError(t, err)
	require.Equal(t, len(line), consumed)

	nameToIdx := make(map[string]int)
	for i, f := range compiled.fields {
		nameToIdx[f.Name] = i
	}

	// Fields that have "-" in this line, so `convertNginxValue` must return nil for each.
	dashFields := []string{"upstream_response_time", "upstream_status", "upstream_connect_time", "upstream_header_time"}
	for _, name := range dashFields {
		idx := nameToIdx[name]
		require.Equal(t, "-", values[idx])
		result, err := convertNginxValue(values[idx], compiled.schema[idx])
		require.NoError(t, err)
		require.Nil(t, result)
	}

	// Check a few non-dash fields to make sure parsing is correct.
	require.Equal(t, "10.0.0.1", values[nameToIdx["remote_addr"]])
	require.Equal(t, "502", values[nameToIdx["status"]])
	require.Equal(t, "MOW", values[nameToIdx["geoip2_region"]])
}

func TestCompileFormatWhitespaceOnly(t *testing.T) {
	_, err := compileFormat("   \t\n  ")
	require.Error(t, err)
}

func TestParseEntryLastVariableNoDelimiter(t *testing.T) {
	compiled, err := compileFormat(`$addr $status`)
	require.NoError(t, err)

	values, consumed, err := compiled.parseEntry("1.2.3.4 200")
	require.NoError(t, err)
	require.Len(t, values, 2)
	require.Equal(t, "1.2.3.4", values[0])
	require.Equal(t, "200", values[1])
	require.Equal(t, 11, consumed)

	values, consumed, err = compiled.parseEntry("1.2.3.4 200\n5.6.7.8 404")
	require.NoError(t, err)
	require.Len(t, values, 2)
	require.Equal(t, "1.2.3.4", values[0])
	require.Equal(t, "200", values[1])
	require.Equal(t, 11, consumed)
}

func TestParseEntryChunkSeparated(t *testing.T) {
	compiled, err := compileFormat(`"$addr" "$status"`)
	require.NoError(t, err)

	chunk1 := `"1.2.3.4`
	chunk2 := `" "200"`

	_, _, err = compiled.parseEntry(chunk1)
	require.Error(t, err)

	values, consumed, err := compiled.parseEntry(chunk1 + chunk2)
	require.NoError(t, err)
	require.Equal(t, len(chunk1)+len(chunk2), consumed)
	require.Equal(t, "1.2.3.4", values[0])
	require.Equal(t, "200", values[1])
}

func TestMatchLiteralWhitespaceFlexibility(t *testing.T) {
	require.Equal(t, 12, matchLiteral("hello\n\tworld", "hello world"))
	require.Equal(t, 12, matchLiteral("hello\r\nworld", "hello world"))
	require.Equal(t, -1, matchLiteral("helloworld", "hello world"))
}

func TestConvertNginxValueBooleanDash(t *testing.T) {
	col := abstract.NewColSchema("flag", schema.TypeBoolean, false)
	result, err := convertNginxValue("-", col)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestUpstreamVariablesAreString(t *testing.T) {
	// Upstream variables may contain comma-separated lists (e.g. "0.1, 0.2")
	// when multiple upstreams are tried, so they must be typed as String.
	upstreamVars := []string{
		"upstream_status", "upstream_response_time", "upstream_connect_time",
		"upstream_header_time", "upstream_response_length",
	}
	for _, name := range upstreamVars {
		_, found := nginxWellKnownTokenTypes[name]
		require.False(t, found, "%s should not be in nginxWellKnownTokenTypes (defaults to String)", name)
	}
}
