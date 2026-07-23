package reference

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_mongo "github.com/transferia/transferia/pkg/providers/mongo"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"github.com/transferia/transferia/pkg/util/set"
	"github.com/transferia/transferia/pkg/worker/tasks"
	canon_test "github.com/transferia/transferia/tests/canon"
	"github.com/transferia/transferia/tests/helpers"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
	"go.mongodb.org/mongo-driver/bson"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
)

// ConductSequenceWithAllSubsequencesTest is the method which MUST be called by concrete sequence checking tests.
// It automatically conducts a test for all subsequences of the given sequence test and canonizes the output.
func ConductSequenceWithAllSubsequencesTest(t *testing.T, sequenceCase canon_test.CanonizedSequenceCase, transfer *model.Transfer, sink abstract.Sinker, sinkAsSource model.Source) {
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), new(model.TransferOperation), transfer, solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}))
	require.NoError(t, snapshotLoader.CleanupSinker(sequenceCase.Tables))

	for i, subSeq := range canon_test.AllSubsequences(sequenceCase.Items) {
		t.Run(
			fmt.Sprintf("subsequence_of_%02d", i+1),
			func(t *testing.T) {
				require.NoError(t, sink.Push(subSeq))
				Dump(t, sinkAsSource)
			},
		)
	}
}

type sourceDumper func(t *testing.T, source model.Source) string

var sourceDumpers = map[reflect.Type]sourceDumper{}

func RegisterSourceDumper[T model.Source](dumper func(t *testing.T, source T) string) {
	var t T
	sourceDumpers[reflect.TypeOf(t)] = func(t *testing.T, source model.Source) string {
		return dumper(t, source.(T))
	}
}

func Dump(t *testing.T, source model.Source) {
	logger.Log.Info(dumpToString(t, source))
	canon.SaveJSON(t, dumpToString(t, source))
}

func dumpToString(t *testing.T, source model.Source) string {
	if dumper, ok := sourceDumpers[reflect.TypeOf(source)]; ok {
		return dumper(t, source)
	}

	switch src := source.(type) {
	case *clickhouse_model.ChSource:
		return FromClickhouse(t, src, false)
	case *provider_yt.YtSource:
		ytClient, err := yt_client.FromConnParams(src, nil)
		require.NoError(t, err)

		res, err := helpers_yt.DumpYtDirectoryToString(ytClient, ypath.Path(src.Paths[0]))
		require.NoError(t, err)
		return res
	case *provider_postgres.PgSource:
		connString, _, err := provider_postgres.PostgresDumpConnString(src)
		require.NoError(t, err)
		args := make([]string, 0)
		args = append(args,
			"--no-publications",
			"--no-subscriptions",
			"--format=plain",
			"--no-owner",
			"--schema-only",
		)
		commandArgs := src.PgDumpCommand[1:]
		commandArgs = append(commandArgs, connString)
		commandArgs = append(commandArgs, args...)
		command := exec.Command(src.PgDumpCommand[0], commandArgs...)
		var stdout, stderr bytes.Buffer
		command.Stdout = &stdout
		command.Stderr = &stderr
		logger.Log.Info("Run pg_dump", log.String("path", command.Path), log.Strings("args", command.Args))
		require.NoError(t, command.Run(), stderr.String())
		conn, err := provider_postgres.MakeConnPoolFromSrc(src, logger.Log)
		require.NoError(t, err)
		defer conn.Close()
		queryRows, err := conn.Query(context.Background(), `
select
    'copy (select * from '||r.relname||' order by '||
    array_to_string(array_agg(distinct a.attname order by a.attname), ',')||
    ') to STDOUT;'
from
    pg_class r,
    pg_constraint c,
    pg_attribute a
where
    r.oid = c.conrelid
    and r.oid = a.attrelid
    and a.attnum = ANY(conkey)
    and contype = 'p'
    and relkind = 'r'
    and r.relname not like '__consumer_keeper'
group by
    r.relname
order by
    r.relname;
`)
		require.NoError(t, err)
		defer queryRows.Close()
		var queries []string
		for queryRows.Next() {
			var query string
			require.NoError(t, queryRows.Scan(&query))
			queries = append(queries, query)
		}
		pgsqlBin := os.Getenv("PG_LOCAL_BIN_PATH") + "/psql"
		for _, query := range queries {
			psqlCmd := exec.Command(pgsqlBin, connString)
			var qstdout, stderr bytes.Buffer
			psqlCmd.Stdout = &qstdout
			psqlCmd.Stderr = &stderr
			psqlCmd.Stdin = bytes.NewReader([]byte(query))
			logger.Log.Info("Run psql", log.String("path", pgsqlBin), log.Array("args", connString))
			require.NoError(t, psqlCmd.Run())
			stdout.WriteString(query + "\n")
			stdout.Write(qstdout.Bytes())
			stdout.WriteString("\n\n")
		}

		return stdout.String()
	case *provider_mysql.MysqlSource:
		dump := helpers.MySQLDump(t, src.ToStorageParams())
		return dump
	case *provider_mongo.MongoSource:
		dumpStart := time.Now()
		logger.Log.Debugf("dumpToString MongoSource START")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		excluded := set.New("admin", "config", "local")
		logger.Log.Debugf("dumpToString MongoSource: Connect START hosts=%v port=%d elapsed=%v", src.Hosts, src.Port, time.Since(dumpStart))
		client, err := provider_mongo.Connect(ctx, src.ConnectionOptions([]string{}), logger.Log)
		require.NoError(t, err)
		defer func() {
			if err := client.Close(ctx); err != nil {
				logger.Log.Errorf("dumpToString MongoSource: client.Close FAILED: %v", err)
			}
		}()
		logger.Log.Debugf("dumpToString MongoSource: Connect DONE elapsed=%v", time.Since(dumpStart))
		logger.Log.Debugf("dumpToString MongoSource: ListDatabases START elapsed=%v", time.Since(dumpStart))
		dbs, err := client.ListDatabases(ctx, bson.D{})
		require.NoError(t, err)
		logger.Log.Debugf("dumpToString MongoSource: ListDatabases DONE count=%d elapsed=%v", len(dbs.Databases), time.Since(dumpStart))
		sort.Slice(dbs.Databases, func(i, j int) bool {
			return dbs.Databases[i].Name < dbs.Databases[j].Name
		})
		buf := bytes.NewBuffer(nil)
		for _, db := range dbs.Databases {
			if excluded.Contains(db.Name) {
				logger.Log.Debugf("dumpToString MongoSource: skip excluded db=%s", db.Name)
				continue
			}
			logger.Log.Debugf("dumpToString MongoSource: ListCollectionNames START db=%s elapsed=%v", db.Name, time.Since(dumpStart))
			collections, err := client.Database(db.Name).ListCollectionNames(ctx, bson.D{})
			require.NoError(t, err)
			logger.Log.Debugf("dumpToString MongoSource: ListCollectionNames DONE db=%s count=%d elapsed=%v", db.Name, len(collections), time.Since(dumpStart))
			sort.Strings(collections)
			for _, collection := range collections {
				buf.WriteString(fmt.Sprintf("\n%s\n", collection))
				logger.Log.Debugf("dumpToString MongoSource: Find START db=%s coll=%s elapsed=%v", db.Name, collection, time.Since(dumpStart))
				rows, err := client.Database(db.Name).Collection(collection).Find(ctx, bson.D{}, mongo_options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
				require.NoError(t, err)
				rowCount := 0
				for rows.Next(ctx) {
					var row interface{}
					require.NoError(t, rows.Decode(&row))
					r, err := bson.MarshalExtJSON(row, false, true)
					require.NoError(t, err)
					var repacked interface{}
					require.NoError(t, json.Unmarshal(r, &repacked))
					raw, err := json.Marshal(repacked)
					require.NoError(t, err)
					_, err = buf.Write(raw)
					require.NoError(t, err)
					_, err = buf.WriteString("\n")
					require.NoError(t, err)
					rowCount++
				}
				require.NoError(t, rows.Close(ctx))
				require.NoError(t, rows.Err())
				logger.Log.Debugf("dumpToString MongoSource: Find DONE db=%s coll=%s rows=%d elapsed=%v", db.Name, collection, rowCount, time.Since(dumpStart))
			}
		}
		logger.Log.Debugf("dumpToString MongoSource ALL DONE elapsed=%v", time.Since(dumpStart))
		logger.Log.Infof("readed data: \n%s", buf.String())
		return buf.String()
	default:
		t.Fatalf("source %T not implement dumper yet", source)
		return ""
	}
}

func FromClickhouse(t *testing.T, src *clickhouse_model.ChSource, noTimeCols bool) string {
	type tableListResponse struct {
		Data []struct {
			FullName string
		}
	}
	var tables tableListResponse
	sinkParams, err := src.ToSinkParams()
	require.NoError(t, err)
	httpClient, err := httpclient.NewHTTPClientImpl(sinkParams)
	require.NoError(t, err)
	connHost := &conn_clickhouse.Host{
		Name:       "localhost",
		HTTPPort:   src.HTTPPort,
		NativePort: src.NativePort,
	}

	require.NoError(t, httpClient.Query(
		context.Background(),
		logger.Log,
		connHost,
		`select '"' || database || '"."' || name || '"' as FullName from system.tables where database not like '%system%' FORMAT JSON`,
		&tables,
	))
	excludeList := ""
	if noTimeCols {
		excludeList = "EXCEPT (__data_transfer_commit_time, __data_transfer_delete_time)"
	}
	buf := bytes.NewBuffer(nil)
	for _, table := range tables.Data {
		if strings.Contains(strings.ToLower(table.FullName), "information_schema") {
			continue
		}
		buf.WriteString(fmt.Sprintf("\n%s\n", table.FullName))
		require.NoError(t, httpClient.Exec(
			context.Background(),
			logger.Log,
			connHost,
			fmt.Sprintf(`OPTIMIZE TABLE %s FINAL`, table.FullName),
		))

		reader, err := httpClient.QueryStream(
			context.Background(),
			logger.Log,
			connHost,
			fmt.Sprintf(`select * %s from %s order by 1 FORMAT JSON`, excludeList, table.FullName),
		)
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		_, err = buf.Write(data)
		require.NoError(t, err)
	}
	return buf.String()
}
