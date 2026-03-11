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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/tests/helpers"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

const (
	testNginxFormat = `"$remote_addr" "-" "$remote_user" "[$time_local]" "$request" "$status" "$body_bytes_sent" "$http_referer" "$http_user_agent" "$bytes_sent"
"$edgename" "$scheme" "$host" "$request_time" "$upstream_response_time" "$request_length" "$http_range" "[$responding_node]"
"$upstream_cache_status" "$upstream_response_length" "$upstream_addr" "$gcdn_api_client_id" "$gcdn_api_resource_id" "$uid_got" "$uid_set"
"$geoip_country_code" "$geoip_city" "$shield_type" "$server_addr" "$server_port" "$upstream_status" "-" "$upstream_connect_time"
"$upstream_header_time" "$shard_addr" "$geoip2_data_asnumber" "$connection" "$connection_requests" "$request_id" "$http_x_forwarded_proto"
"$http_x_forwarded_request_id" "$ssl_cipher" "$ssl_session_id" "$ssl_session_reused" "$sent_http_content_type" "$real_tcpinfo_rtt"
"$http_x_forwarded_http_ver" "$vp_enabled" "$geoip2_region"`

	testBucketName = "nginx-test"
	testPathPrefix = "nginx_logs"

	TableNamespace = "nginx"
	TableName      = "access_log"
)

var sampleNginxLines = []string{
	`"212.28.183.93" "-" "-" "[28/Nov/2025:10:04:24 +0000]" "GET /wp-content/mysql.sql HTTP/1.1" "403" "209" "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36" "483" "[blt-up-gc15]" "http" "linkpayment-test.tbank.ru_cache_sharded" "0.085" "0.074" "687" "-" "[blt]" "MISS" "240" "178.130.128.60:443" "875" "5374336" "-" "-" "US" "St Louis" "shield_no" "193.17.93.194" "10080" "403" "-" "0.047" "0.073" "5.188.7.13" "40021" "3210962417" "5" "fd9206997a94e9d81ddb37515c09199c" "https" "8b37970ca1ef7398ae68e6113969633d" "-" "-" "-" "application/xml" "132957" "HTTP/1.1" "0" "MO"`,
	`"212.28.183.93" "-" "-" "[28/Nov/2025:10:04:24 +0000]" "GET /v2/_catalog HTTP/1.1" "200" "5275" "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36" "5705" "[blt-up-gc15]" "http" "linkpayment-test.tbank.ru_cache_sharded" "0.108" "0.099" "678" "-" "[blt]" "MISS" "15701" "178.130.128.60:443" "875" "5374336" "-" "-" "US" "St Louis" "shield_no" "193.17.93.194" "10080" "200" "-" "0.048" "0.096" "5.188.7.13" "40021" "3210963121" "2" "4afb934439dfebe52fd91043e8fbefca" "https" "fc7bc5a0360089538e9afbf965ceebcb" "-" "-" "-" "text/html" "134812" "HTTP/1.1" "0" "MO"`,
	`"212.28.183.93" "-" "-" "[28/Nov/2025:10:04:24 +0000]" "GET /phpinfo.php HTTP/1.1" "403" "207" "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36" "481" "[blt-up-gc15]" "http" "linkpayment-test.tbank.ru_cache_sharded" "0.132" "0.089" "678" "-" "[blt]" "MISS" "240" "178.130.128.60:443" "875" "5374336" "-" "-" "US" "St Louis" "shield_no" "193.17.93.194" "10080" "403" "-" "0.052" "0.087" "5.188.7.13" "40021" "3210963148" "2" "bb7645f6ca6b91f49c718541673ceb23" "https" "8f49c72792e7daf6be5e151b3ea9f84c" "-" "-" "-" "application/xml" "129724" "HTTP/1.1" "0" "MO"`,
	`"212.28.183.93" "-" "-" "[28/Nov/2025:10:04:24 +0000]" "GET /actuator/heapdump HTTP/1.1" "200" "5275" "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36" "5705" "[blt-up-gc15]" "http" "linkpayment-test.tbank.ru_cache_sharded" "0.132" "0.118" "684" "-" "[blt]" "MISS" "15701" "178.130.128.60:443" "875" "5374336" "-" "-" "US" "St Louis" "shield_no" "193.17.93.194" "10080" "200" "-" "0.051" "0.118" "5.188.7.13" "40021" "3210958231" "20" "562514df31224ea33d37b7f8b54b2a16" "https" "d9ce359663c40bbb6e9f97e5a3f81e31" "-" "-" "-" "text/html" "132120" "HTTP/1.1" "0" "MO"`,

	// Multi-lined entry.
	`"0.0.0.0" "-" "-" "[26/Apr/2019:09:47:40 +0000]" "GET /ContentCommon/images/image.png HTTP/1.1" "200" "1514283" "https://example.com/videos/10"
"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 YaBrowser/16.10.0.2309 Safari/537.36"
"1514848" "[dh-up-gc18]" "https" "cdn.example.com" "1.500" "0.445" "157" "bytes=0-1901653" "[dh]" "MISS" "10485760" "0.0.0.0:80" "2510" "7399" "-"
"-" "KZ" "-" "shield_no" "0.0.0.0" "80" "206" "-" "0.000" "0.200" "0.0.0.0" "asnumber" "106980391" "1" "c1c0f12ab35b7cccccd5dc0a454879c5" "-" "-"
"ECDHE-RSA-AES256-GCM-SHA384" "28a4184139cb43cdc79006cf2d1a4ac93bdc****" "r" "application/json" "11863" "HTTP/1.1" "1" "AMU"`,

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

func buildSourceModel(t *testing.T) *s3.S3Source {
	src := s3recipe.PrepareCfg(t, testBucketName, dp_model.ParsingFormatNginx)
	src.PathPrefix = testPathPrefix
	src.Bucket = testBucketName
	src.TableNamespace = TableNamespace
	src.TableName = TableName
	src.Format.NginxSetting = &s3.NginxSetting{Format: testNginxFormat}
	src.WithDefaults()
	s3recipe.CreateBucket(t, src)
	s3recipe.UploadOneFromMemory(t, src, testPathPrefix+"/access_snap.log.gz", makeGzipData(t, sampleNginxLines))
	return src
}

func TestNginxSnapshot(t *testing.T) {
	src := buildSourceModel(t)
	dst := makeDst()
	transfer := helpers.MakeTransfer("snap", src, &dst, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, &dst, TableNamespace, TableName, uint64(len(sampleNginxLines)))
	canonDst(t, dst, transfer)
}

func TestNginxIncrement(t *testing.T) {
	src := buildSourceModel(t)
	dst := makeDst()
	transfer := helpers.MakeTransfer("incr", src, &dst, abstract.TransferTypeIncrementOnly)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	s3recipe.UploadOneFromMemory(t, src, testPathPrefix+"/access_incr.log.gz", makeGzipData(t, sampleNginxLines))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		TableNamespace, TableName,
		helpers.GetSampleableStorageByModel(t, transfer.Dst),
		5*time.Minute,
		uint64(len(sampleNginxLines)),
	))
	canonDst(t, dst, transfer)
}

func canonDst(t *testing.T, dst model.ChDestination, transfer *dp_model.Transfer) {
	cfg, err := dst.ToSinkParams(transfer)
	require.NoError(t, err)
	opt, err := conn.GetClickhouseOptions(cfg, []*chconn.Host{{
		Name:       dst.ShardsList[0].Hosts[0],
		NativePort: dst.NativePort,
	}})
	require.NoError(t, err)
	db := clickhouse.OpenDB(opt)
	defer db.Close()
	toCanon := make(map[string]any)
	var ddl string
	require.NoError(t, db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s.%s", TableNamespace, TableName)).Scan(&ddl))
	toCanon["ddl"] = ddl
	toCanon["data"] = querySelectAll(t, db, TableNamespace+"."+TableName)
	canon.SaveJSON(t, toCanon)
}

func makeDst() model.ChDestination {
	dst := model.ChDestination{
		ShardsList:          []model.ClickHouseShard{{Name: "_", Hosts: []string{"localhost"}}},
		User:                "default",
		Database:            TableNamespace,
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             dp_model.Drop,
	}
	dst.WithDefaults()
	return dst
}

func querySelectAll(t *testing.T, conn *sql.DB, fqtn string) string {
	rows, err := conn.Query("SELECT * EXCEPT(__file_name) FROM " + fqtn + " ORDER BY request_id")
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
