package reference

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/providers/mysql"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util/set"
	"github.com/transferia/transferia/pkg/worker/tasks"
	dt_canon "github.com/transferia/transferia/tests/canon"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.ytsaurus.tech/library/go/core/log"
)

// ConductSequenceWithAllSubsequencesTest is the method which MUST be called by concrete sequence checking tests.
// It automatically conducts a test for all subsequences of the given sequence test and canonizes the output.
func ConductSequenceWithAllSubsequencesTest(t *testing.T, sequenceCase dt_canon.CanonizedSequenceCase, transfer *dp_model.Transfer, sink abstract.Sinker, sinkAsSource dp_model.Source) {
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}))
	require.NoError(t, snapshotLoader.CleanupSinker(sequenceCase.Tables))

	for i, subSeq := range dt_canon.AllSubsequences(sequenceCase.Items) {
		t.Run(
			fmt.Sprintf("subsequence_of_%02d", i+1),
			func(t *testing.T) {
				require.NoError(t, sink.Push(subSeq))
				Dump(t, sinkAsSource)
			},
		)
	}
}

func Dump(t *testing.T, source dp_model.Source) {
	logger.Log.Info(dumpToString(t, source))
	canon.SaveJSON(t, dumpToString(t, source))
}

func dumpToString(t *testing.T, source dp_model.Source) string {
	switch src := source.(type) {
	case *model.ChSource:
		return FromClickhouse(t, src, false)
	case *pgcommon.PgSource:
		connString, _, err := pgcommon.PostgresDumpConnString(src)
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
		conn, err := pgcommon.MakeConnPoolFromSrc(src, logger.Log)
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
	case *mysql.MysqlSource:
		dump := helpers.MySQLDump(t, src.ToStorageParams())
		return dump
	case *mongo.MongoSource:
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		excluded := set.New("admin", "config", "local")
		client, err := mongo.Connect(ctx, src.ConnectionOptions([]string{}), logger.Log)
		require.NoError(t, err)
		dbs, err := client.ListDatabases(ctx, bson.D{})
		require.NoError(t, err)
		buf := bytes.NewBuffer(nil)
		for _, db := range dbs.Databases {
			if excluded.Contains(db.Name) {
				continue
			}
			collections, err := client.Database(db.Name).ListCollectionNames(ctx, bson.D{})
			require.NoError(t, err)
			sort.Strings(collections)
			for _, collection := range collections {
				buf.WriteString(fmt.Sprintf("\n%s\n", collection))
				rows, err := client.Database(db.Name).Collection(collection).Find(ctx, bson.D{})
				require.NoError(t, err)
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
				}
				require.NoError(t, rows.Close(ctx))
				require.NoError(t, rows.Err())
			}
		}
		logger.Log.Infof("readed data: \n%s", buf.String())
		return buf.String()
	default:
		t.Fatalf("source %T not implement dumper yet", source)
		return ""
	}
}

func FromClickhouse(t *testing.T, src *model.ChSource, noTimeCols bool) string {
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
	connHost := &chconn.Host{
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
