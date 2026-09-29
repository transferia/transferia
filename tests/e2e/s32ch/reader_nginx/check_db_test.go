package nginx

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	_ "github.com/transferia/transferia/tests/helpers/registration/s3"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

const (
	testNginxFormat = `"$remote_addr" "-" "$remote_user" "[$date_time]" "$request" "$status_code" "$body_bytes_sent" "$http_referer" "$http_user_agent" "$bytes_sent"
"[$edgename]" "$scheme" "$host" "$request_time" "$upstream_response_time" "$request_length" "$http_range" "[$responding_node]"
"$upstream_cache_status" "$upstream_response_length" "$upstream_addr" "$gcdn_api_client_id" "$gcdn_api_resource_id" "$uid_got" "$uid_set"
"$geoip_country_code" "$geoip_city" "$shield_type" "$server_addr" "$server_port" "$upstream_status" "-" "$upstream_connect_time"
"$upstream_header_time" "$shard_addr" "$geoip2_data_asnumber" "$connection" "$connection_requests" "$request_id" "$http_x_forwarded_proto"
"$http_x_forwarded_request_id" "$ssl_cipher" "$ssl_session_id" "$ssl_session_reused" "$sent_http_content_type" "$tcpinfo_rtt"
"$http_x_forwarded_http_ver" "$vp_enabled" "$geoip2_region"`

	testBucketName = "nginx-test"
	testPathPrefix = "nginx_logs"

	TableNamespace = "nginx"
	TableName      = "access_log"
)

var sampleNginxLines = []string{
	`"95.104.168.83" "-" "-" "[06/Apr/2026:05:01:57 +0000]" "GET /v1/?platform=android HTTP/2.0" "200" "32141" "-" "Xiaomi 2209116AG/android: 13/TCSMB/6.28.0" "32617" "[m9p-up-gc30]" "https" "certs.tinkoff.ru" "0.000" "-" "81" "-" "[m9p]" "HIT" "-" "-" "875" "158571" "-" "-" "RU" "Saratov" "shield_no" "193.17.93.93" "443" "-" "-" "-" "-" "-" "29190" "7998725539" "1" "2810577465de6219624300592265177a" "-" "-" "TLS_AES_256_GCM_SHA384" "4c3de986d29066cff22c237ad6fa442fdd2084892cc22bde675060a418172830" "." "application/json" "30444" "-" "0" "SAR" "TLSv1.3" "TLS_AES_256_GCM_SHA384"`,
	`"45.83.21.114" "-" "-" "[06/Apr/2026:05:01:57 +0000]" "GET /v1/?platform=android HTTP/2.0" "200" "32141" "-" "okhttp/4.10.0" "32617" "[m9p-up-gc30]" "https" "certs.tinkoff.ru" "0.000" "-" "51" "-" "[m9p]" "HIT" "-" "-" "875" "158571" "-" "-" "CY" "-" "shield_no" "193.17.93.93" "443" "-" "-" "-" "-" "-" "209847" "7998724274" "1" "945a0ce1843c7e4e3ef9b4f9a18fbea8" "-" "-" "TLS_AES_256_GCM_SHA384" "ffa5e70ca449ce326bb8bd51a810880b98671a623b864818d918068ccdf45eba" "." "application/json" "79782" "-" "0" "-" "TLSv1.3" "TLS_AES_256_GCM_SHA384"`,
	`"45.83.21.114" "-" "-" "[06/Apr/2026:05:01:57 +0000]" "GET /v1/?platform=android HTTP/2.0" "200" "32141" "-" "okhttp/4.10.0" "32206" "[m9p-up-gc30]" "https" "certs.tinkoff.ru" "0.011" "-" "23" "-" "[m9p]" "HIT" "-" "-" "875" "158571" "-" "-" "CY" "-" "shield_no" "193.17.93.93" "443" "-" "-" "-" "-" "-" "209847" "7998724274" "2" "337b8d60403f502e5c9f79530d22f1ee" "-" "-" "TLS_AES_256_GCM_SHA384" "ffa5e70ca449ce326bb8bd51a810880b98671a623b864818d918068ccdf45eba" "." "application/json" "79782" "-" "0" "-" "TLSv1.3" "TLS_AES_256_GCM_SHA384"`,
	`"84.42.72.121" "-" "-" "[06/Apr/2026:05:01:57 +0000]" "GET /v1/?platform=android HTTP/2.0" "200" "32141" "-" "okhttp/4.10.0" "32617" "[m9p-up-gc30]" "https" "certs.tinkoff.ru" "0.000" "-" "51" "-" "[m9p]" "HIT" "-" "-" "875" "158571" "-" "-" "RU" "Bryansk" "shield_no" "193.17.93.93" "443" "-" "-" "-" "-" "-" "12389" "7998726116" "1" "cb1eb8fe76ff6db1143d11220d3eedf3" "-" "-" "TLS_AES_256_GCM_SHA384" "fcb9b09c615ae62f940731c3f9b3fc1b6023323aba77cf97535ca73a1dd3b9e2" "." "application/json" "32875" "-" "0" "BRY" "TLSv1.3" "TLS_AES_256_GCM_SHA384"`,
	`"84.42.72.121" "-" "-" "[06/Apr/2026:05:01:57 +0000]" "GET /v1/?platform=android HTTP/2.0" "200" "32141" "-" "okhttp/4.10.0" "32206" "[m9p-up-gc30]" "https" "certs.tinkoff.ru" "0.000" "-" "23" "-" "[m9p]" "HIT" "-" "-" "875" "158571" "-" "-" "RU" "Bryansk" "shield_no" "193.17.93.93" "443" "-" "-" "-" "-" "-" "12389" "7998726116" "2" "f24a0201f1228d0e8b2ad7ebcc59c19f" "-" "-" "TLS_AES_256_GCM_SHA384" "fcb9b09c615ae62f940731c3f9b3fc1b6023323aba77cf97535ca73a1dd3b9e2" "." "application/json" "33703" "-" "0" "BRY" "TLSv1.3" "TLS_AES_256_GCM_SHA384"`,
	`"176.59.33.27" "-" "-" "[06/Apr/2026:05:01:57 +0000]" "GET /v1/?platform=android HTTP/2.0" "200" "32141" "-" "Xiaomi 22126RN91Y/android: 13/TCSMB/6.31.0" "32617" "[m9p-up-gc30]" "https" "certs.tinkoff.ru" "0.000" "-" "82" "-" "[m9p]" "HIT" "-" "-" "875" "158571" "-" "-" "RU" "-" "shield_no" "193.17.93.93" "443" "-" "-" "-" "-" "-" "12958" "7998726683" "1" "360f213898864d76de15a5f0c912c6fd" "-" "-" "TLS_AES_256_GCM_SHA384" "4b4008c6071320274335f17dede8e1dd2cd2df86091818a61f7843b345362d5a" "." "application/json" "37691" "-" "0" "KRS" "TLSv1.3" "TLS_AES_256_GCM_SHA384"`,
	`"176.59.48.114" "-" "-" "[06/Apr/2026:05:01:57 +0000]" "GET /v1/?platform=android HTTP/2.0" "200" "32141" "-" "INFINIX Infinix X6833B/android: 14/TCSMB/6.27.0" "32617" "[m9p-up-gc30]" "https" "certs.tinkoff.ru" "0.000" "-" "88" "-" "[m9p]" "HIT" "-" "-" "875" "158571" "-" "-" "RU" "-" "shield_no" "193.17.93.93" "443" "-" "-" "-" "-" "-" "12958" "7998727002" "1" "cf6d0a3ba5789149374a2fe223ca9c95" "-" "-" "TLS_AES_256_GCM_SHA384" "a54afa89243749b04cf10907a14c753e96f28899478a0c6604617ad888532c1d" "." "application/json" "36195" "-" "0" "-" "TLSv1.3" "TLS_AES_256_GCM_SHA384"`,
	`"91.78.129.251" "-" "-" "[06/Apr/2026:05:01:57 +0000]" "GET /v1/?platform=android HTTP/2.0" "200" "32141" "-" "Xiaomi 2209116AG/android: 13/TCSMB/6.31.0" "32617" "[m9p-up-gc30]" "https" "certs.tinkoff.ru" "0.000" "-" "81" "-" "[m9p]" "HIT" "-" "-" "875" "158571" "-" "-" "RU" "St Petersburg" "shield_no" "193.17.93.93" "443" "-" "-" "-" "-" "-" "8359" "7998727963" "1" "e8c098edb444f54061bc3c475bd76bba" "-" "-" "TLS_AES_256_GCM_SHA384" "b62c3403991c2fbe9531d65a5e78cb170a9caaa97c4c696be4e5529c412bfb87" "." "application/json" "41199" "-" "0" "SPE" "TLSv1.3" "TLS_AES_256_GCM_SHA384"`,
	`"128.71.121.131" "-" "-" "[06/Apr/2026:05:01:58 +0000]" "GET /v1/?platform=android HTTP/2.0" "200" "32141" "-" "TECNO LH7n/android: 14/TCSMB/6.36.0" "32617" "[m9p-up-gc30]" "https" "certs.tinkoff.ru" "0.000" "-" "79" "-" "[m9p]" "HIT" "-" "-" "875" "158571" "-" "-" "RU" "Moscow" "shield_no" "193.17.93.93" "443" "-" "-" "-" "-" "-" "16345" "7998728286" "1" "fd8c6f2723935a3fc7ec577ad89617eb" "-" "-" "TLS_AES_256_GCM_SHA384" "11e26571b9ab3a9395c575019376adc77bfdd0a6a053aab7af02e7d8f1cbd770" "." "application/json" "26602" "-" "0" "MOW" "TLSv1.3" "TLS_AES_256_GCM_SHA384"`,

	`"212.28.183.93" "-" "-" "[28/Nov/2025:10:04:24 +0000]" "GET /wp-content/mysql.sql HTTP/1.1" "403" "209" "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36" "483" "[blt-up-gc15]" "http" "linkpayment-test.tbank.ru_cache_sharded" "0.085" "0.074" "687" "-" "[blt]" "MISS" "240" "178.130.128.60:443" "875" "5374336" "-" "-" "US" "St Louis" "shield_no" "193.17.93.194" "10080" "403" "-" "0.047" "0.073" "5.188.7.13" "40021" "3210962417" "5" "fd9206997a94e9d81ddb37515c09199c" "https" "8b37970ca1ef7398ae68e6113969633d" "-" "-" "-" "application/xml" "132957" "HTTP/1.1" "0" "MO"`,
	`"212.28.183.93" "-" "-" "[28/Nov/2025:10:04:24 +0000]" "GET /v2/_catalog HTTP/1.1" "200" "5275" "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36" "5705" "[blt-up-gc15]" "http" "linkpayment-test.tbank.ru_cache_sharded" "0.108" "0.099" "678" "-" "[blt]" "MISS" "15701" "178.130.128.60:443" "875" "5374336" "-" "-" "US" "St Louis" "shield_no" "193.17.93.194" "10080" "200" "-" "0.048" "0.096" "5.188.7.13" "40021" "3210963121" "2" "4afb934439dfebe52fd91043e8fbefca" "https" "fc7bc5a0360089538e9afbf965ceebcb" "-" "-" "-" "text/html" "134812" "HTTP/1.1" "0" "MO"`,
	`"212.28.183.93" "-" "-" "[28/Nov/2025:10:04:24 +0000]" "GET /phpinfo.php HTTP/1.1" "403" "207" "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36" "481" "[blt-up-gc15]" "http" "linkpayment-test.tbank.ru_cache_sharded" "0.132" "0.089" "678" "-" "[blt]" "MISS" "240" "178.130.128.60:443" "875" "5374336" "-" "-" "US" "St Louis" "shield_no" "193.17.93.194" "10080" "403" "-" "0.052" "0.087" "5.188.7.13" "40021" "3210963148" "2" "bb7645f6ca6b91f49c718541673ceb23" "https" "8f49c72792e7daf6be5e151b3ea9f84c" "-" "-" "-" "application/xml" "129724" "HTTP/1.1" "0" "MO"`,
	`"212.28.183.93" "-" "-" "[28/Nov/2025:10:04:24 +0000]" "GET /actuator/heapdump HTTP/1.1" "200" "5275" "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36" "5705" "[blt-up-gc15]" "http" "linkpayment-test.tbank.ru_cache_sharded" "0.132" "0.118" "684" "-" "[blt]" "MISS" "15701" "178.130.128.60:443" "875" "5374336" "-" "-" "US" "St Louis" "shield_no" "193.17.93.194" "10080" "200" "-" "0.051" "0.118" "5.188.7.13" "40021" "3210958231" "20" "562514df31224ea33d37b7f8b54b2a16" "https" "d9ce359663c40bbb6e9f97e5a3f81e31" "-" "-" "-" "text/html" "132120" "HTTP/1.1" "0" "MO"`,

	// Entry with dash values in numbers (upstream_response_time, upstream_connect_time, upstream_header_time).
	`"10.0.0.1" "-" "-" "[28/Nov/2025:10:05:00 +0000]" "GET /health HTTP/1.1" "502" "0" "-" "curl/7.68.0" "256" "[edge-node]" "https" "api.example.com" "0.001" "-" "45" "-" "[origin]" "-" "-" "-" "100" "200" "-" "-" "RU" "Moscow" "shield_no" "10.0.0.1" "443" "-" "-" "-" "-" "10.0.0.2" "12345" "999999" "1" "abc123" "https" "def456" "TLS_AES_128_GCM_SHA256" "-" "-" "text/plain" "5000" "HTTP/2.0" "0" "MOW"`,
}

func makeGzipData(t *testing.T, lines []string) []byte {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write([]byte(strings.Join(lines, "\n") + "\n"))
	require.NoError(t, err)
	require.NoError(t, gw.Close())
	return buf.Bytes()
}

func buildSourceModel(t *testing.T) *s3_model.S3Source {
	src := s3recipe.PrepareCfg(t, testBucketName, model.ParsingFormatNginx)
	src.PathPrefix = testPathPrefix
	src.Bucket = testBucketName
	src.TableNamespace = TableNamespace
	src.TableName = TableName
	src.Format.NginxSetting = &s3_model.NginxSetting{Format: testNginxFormat}
	src.WithDefaults()
	src.UnparsedPolicy = s3_model.UnparsedPolicyFail
	src.OutputSchema = []abstract.ColSchema{
		{ColumnName: "remote_addr", DataType: "string", PrimaryKey: true},
		{ColumnName: "dash1", DataType: "string"},
		{ColumnName: "remote_user", DataType: "string"},
		{ColumnName: "date_time", DataType: "datetime", PrimaryKey: true},
		{ColumnName: "request", DataType: "string", PrimaryKey: true},
		{ColumnName: "status_code", DataType: "int64"},
		{ColumnName: "body_bytes_sent", DataType: "int64"},
		{ColumnName: "http_referer", DataType: "string"},
		{ColumnName: "http_user_agent", DataType: "string"},
		{ColumnName: "bytes_sent", DataType: "int64"},
		{ColumnName: "edgename", DataType: "string"},
		{ColumnName: "scheme", DataType: "string"},
		{ColumnName: "host", DataType: "string", PrimaryKey: true},
		{ColumnName: "request_time", DataType: "double"},
		{ColumnName: "upstream_response_time", DataType: "string"},
		{ColumnName: "request_length", DataType: "int64"},
		{ColumnName: "http_range", DataType: "string"},
		{ColumnName: "responding_node", DataType: "string"},
		{ColumnName: "upstream_cache_status", DataType: "string"},
		{ColumnName: "upstream_response_length", DataType: "string"},
		{ColumnName: "upstream_addr", DataType: "string"},
		{ColumnName: "gcdn_api_client_id", DataType: "string"},
		{ColumnName: "gcdn_api_resource_id", DataType: "string"},
		{ColumnName: "uid_got", DataType: "string"},
		{ColumnName: "uid_set", DataType: "string"},
		{ColumnName: "geoip_country_code", DataType: "string"},
		{ColumnName: "geoip_city", DataType: "string"},
		{ColumnName: "shield_type", DataType: "string"},
		{ColumnName: "server_addr", DataType: "string"},
		{ColumnName: "server_port", DataType: "int64"},
		{ColumnName: "upstream_status", DataType: "string"},
		{ColumnName: "dash2", DataType: "string"},
		{ColumnName: "upstream_connect_time", DataType: "string"},
		{ColumnName: "upstream_header_time", DataType: "string"},
		{ColumnName: "shard_addr", DataType: "string"},
		{ColumnName: "geoip2_data_asnumber", DataType: "string"},
		{ColumnName: "connection", DataType: "int64"},
		{ColumnName: "connection_requests", DataType: "int64"},
		{ColumnName: "request_id", DataType: "string"},
		{ColumnName: "http_x_forwarded_proto", DataType: "string"},
		{ColumnName: "http_x_forwarded_request_id", DataType: "string"},
		{ColumnName: "ssl_cipher", DataType: "string"},
		{ColumnName: "ssl_session_id", DataType: "string"},
		{ColumnName: "ssl_session_reused", DataType: "string"},
		{ColumnName: "sent_http_content_type", DataType: "string"},
		{ColumnName: "tcpinfo_rtt", DataType: "string"},
		{ColumnName: "http_x_forwarded_http_ver", DataType: "string"},
		{ColumnName: "vp_enabled", DataType: "string"},
		{ColumnName: "geoip2_region", DataType: "string"},
	}
	s3recipe.CreateBucket(t, src)
	s3recipe.UploadOneFromMemory(t, src, testPathPrefix+"/access_snap.log.gz", makeGzipData(t, sampleNginxLines))
	return src
}

func TestNginxSnapshot(t *testing.T) {
	src := buildSourceModel(t)
	dst := makeDst()
	transfer := transferhelpers.MakeTransfer("snap", src, &dst, abstract.TransferTypeSnapshotOnly)
	delivery.Activate(t, transfer)
	storagecomparison.CheckRowsCount(t, &dst, TableNamespace, TableName, uint64(len(sampleNginxLines)))
	canonDst(t, dst, transfer)
}

func TestNginxIncrement(t *testing.T) {
	src := buildSourceModel(t)
	dst := makeDst()
	transfer := transferhelpers.MakeTransfer("incr", src, &dst, abstract.TransferTypeIncrementOnly)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	s3recipe.UploadOneFromMemory(t, src, testPathPrefix+"/access_incr.log.gz", makeGzipData(t, sampleNginxLines))
	require.NoError(t, storage.WaitDestinationEqualRowsCount(
		TableNamespace, TableName,
		storagecomparison.GetSampleableStorageByModel(t, transfer.Dst),
		5*time.Minute,
		uint64(len(sampleNginxLines)),
	))
	canonDst(t, dst, transfer)
}

func canonDst(t *testing.T, dst clickhouse_model.ChDestination, transfer *model.Transfer) {
	cfg, err := dst.ToSinkParams(transfer)
	require.NoError(t, err)
	opt, err := conn.GetClickhouseOptions(cfg, []*conn_clickhouse.Host{{
		Name:       dst.ShardsList[0].Hosts[0],
		NativePort: dst.NativePort,
	}})
	require.NoError(t, err)
	db := clickhouse_go.OpenDB(opt)
	defer db.Close()
	toCanon := make(map[string]any)
	var ddl string
	require.NoError(t, db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s.%s", TableNamespace, TableName)).Scan(&ddl))
	toCanon["ddl"] = ddl
	toCanon["data"] = querySelectAll(t, db, TableNamespace+"."+TableName)
	canon.SaveJSON(t, toCanon)
}

func makeDst() clickhouse_model.ChDestination {
	dst := clickhouse_model.ChDestination{
		ShardsList:          []clickhouse_model.ClickHouseShard{{Name: "_", Hosts: []string{"localhost"}}},
		User:                "default",
		Database:            TableNamespace,
		HTTPPort:            testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             model.Drop,
	}
	dst.WithDefaults()
	return dst
}

func querySelectAll(t *testing.T, conn *sql.DB, fqtn string) string {
	rows, err := conn.Query("SELECT * EXCEPT(__file_name, _timestamp) FROM " + fqtn + " ORDER BY request_id")
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	buf := strings.Builder{}
	buf.WriteString(strings.Join(cols, ",") + "\n")
	dest, ptrs := make([]any, len(cols)), make([]any, len(cols))
	for i := range dest {
		ptrs[i] = &dest[i]
	}
	for rows.Next() {
		require.NoError(t, rows.Scan(ptrs...))
		vals := make([]string, len(cols))
		for i, v := range dest {
			if v == nil {
				vals[i] = "NULL"
			} else {
				vals[i] = fmt.Sprint(v)
			}
		}
		buf.WriteString(strings.Join(vals, ",") + "\n")
	}
	require.NoError(t, rows.Err())
	return buf.String()
}
