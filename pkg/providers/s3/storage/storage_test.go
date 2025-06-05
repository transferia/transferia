package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/predicate"
	"github.com/transferia/transferia/pkg/providers/s3"
)

func TestCanonParquet(t *testing.T) {
	testCasePath := "yellow_taxi"
	cfg := s3.PrepareCfg(t, "data3", "")
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	cfg.ReadBatchSize = 100_000
	storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	schema, err := storage.TableList(nil)
	require.NoError(t, err)
	tid := *abstract.NewTableID("test_namespace", "test_name")
	for _, col := range schema[tid].Schema.Columns() {
		logger.Log.Infof("resolved schema: %s (%s) %v", col.ColumnName, col.DataType, col.PrimaryKey)
	}
	totalRows, err := storage.ExactTableRowsCount(tid)
	require.NoError(t, err)
	logger.Log.Infof("estimate %v rows", totalRows)
	require.Equal(t, 12554664, int(totalRows))
	tdesc, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
	require.NoError(t, err)
	require.Equal(t, len(tdesc), 4)
	wg := sync.WaitGroup{}
	wg.Add(len(tdesc))
	cntr := &atomic.Int64{}
	fileSnippets := map[abstract.TableDescription]abstract.TypedChangeItem{}
	for _, desc := range tdesc {
		go func(desc abstract.TableDescription) {
			defer wg.Done()
			require.NoError(
				t,
				storage.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
					if _, ok := fileSnippets[desc]; !ok {
						fileSnippets[desc] = abstract.TypedChangeItem(items[0])
					}
					logger.Log.Infof("pushed: \n%s", abstract.Sniff(items))
					_ = cntr.Add(int64(len(items)))
					return nil
				}),
			)
		}(desc)
	}
	wg.Wait()
	require.Equal(t, int(totalRows), int(cntr.Load()))

	var totalCanon []string
	for desc, sample := range fileSnippets {
		sample.CommitTime = 0
		rawJSON, err := json.MarshalIndent(&sample, "", "    ")
		require.NoError(t, err)
		operands, err := predicate.InclusionOperands(desc.Filter, s3FileNameCol)
		require.NoError(t, err)
		require.Len(t, operands, 1)

		canonData := fmt.Sprintf("file: %s\n%s", operands[0].Val, string(rawJSON))
		require.NoError(t, err)
		fmt.Println(canonData)
		totalCanon = append(totalCanon, canonData)
	}
	sort.Strings(totalCanon)
	canon.SaveJSON(t, strings.Join(totalCanon, "\n"))
}

func TestCanonJsonline(t *testing.T) {
	testCasePath := "test_jsonline_files"
	cfg := s3.PrepareCfg(t, "jsonlinecanon", model.ParsingFormatJSONLine)
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	cfg.ReadBatchSize = 100_000
	cfg.Format.JSONLSetting = new(s3.JSONLSetting)
	cfg.Format.JSONLSetting.BlockSize = 100_000
	storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	schema, err := storage.TableList(nil)
	require.NoError(t, err)
	tid := *abstract.NewTableID("test_namespace", "test_name")
	for _, col := range schema[tid].Schema.Columns() {
		logger.Log.Infof("resolved schema: %s (%s) %v", col.ColumnName, col.DataType, col.PrimaryKey)
	}

	tdesc, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(len(tdesc))
	cntr := &atomic.Int64{}
	fileSnippets := map[abstract.TableDescription]abstract.TypedChangeItem{}
	for _, desc := range tdesc {
		go func(desc abstract.TableDescription) {
			defer wg.Done()
			require.NoError(
				t,
				storage.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
					if _, ok := fileSnippets[desc]; !ok {
						fileSnippets[desc] = abstract.TypedChangeItem(items[0])
					}
					logger.Log.Infof("pushed: \n%s", abstract.Sniff(items))
					_ = cntr.Add(int64(len(items)))
					return nil
				}),
			)
		}(desc)
	}
	wg.Wait()

	var totalCanon []string
	for desc, sample := range fileSnippets {
		sample.CommitTime = 0
		rawJSON, err := json.MarshalIndent(&sample, "", "    ")
		require.NoError(t, err)
		operands, err := predicate.InclusionOperands(desc.Filter, s3FileNameCol)
		require.NoError(t, err)
		require.Len(t, operands, 1)

		canonData := fmt.Sprintf("file: %s\n%s", operands[0].Val, string(rawJSON))
		require.NoError(t, err)
		fmt.Println(canonData)
		totalCanon = append(totalCanon, canonData)
	}
	sort.Strings(totalCanon)
	canon.SaveJSON(t, strings.Join(totalCanon, "\n"))
}

func TestCanonCsv(t *testing.T) {
	testCasePath := "test_csv_large"
	cfg := s3.PrepareCfg(t, "csv_canon", model.ParsingFormatCSV)
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	cfg.ReadBatchSize = 100_000_0
	cfg.Format.CSVSetting = new(s3.CSVSetting)
	cfg.Format.CSVSetting.BlockSize = 100_000_0
	cfg.Format.CSVSetting.Delimiter = ","
	cfg.Format.CSVSetting.QuoteChar = "\""
	cfg.Format.CSVSetting.EscapeChar = "\\"
	storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	schema, err := storage.TableList(nil)
	require.NoError(t, err)
	tid := *abstract.NewTableID("test_namespace", "test_name")
	for _, col := range schema[tid].Schema.Columns() {
		logger.Log.Infof("resolved schema: %s (%s) %v", col.ColumnName, col.DataType, col.PrimaryKey)
	}

	tdesc, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(len(tdesc))
	cntr := &atomic.Int64{}
	fileSnippets := map[abstract.TableDescription]abstract.TypedChangeItem{}
	for _, desc := range tdesc {
		go func(desc abstract.TableDescription) {
			defer wg.Done()
			require.NoError(
				t,
				storage.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
					if _, ok := fileSnippets[desc]; !ok {
						fileSnippets[desc] = abstract.TypedChangeItem(items[0])
					}
					logger.Log.Infof("pushed: \n%s", abstract.Sniff(items))
					_ = cntr.Add(int64(len(items)))
					return nil
				}),
			)
		}(desc)
	}
	wg.Wait()

	rows, err := storage.EstimateTableRowsCount(tid)
	require.NoError(t, err)
	diff := math.Abs(float64(cntr.Load()) - float64(rows))
	percent := (diff * 100) / float64(cntr.Load())
	// check that row estimation is at most 5 % off
	require.Less(t, percent, float64(5))

	require.Equal(t, 500000, int(cntr.Load()))

	var totalCanon []string
	for desc, sample := range fileSnippets {
		sample.CommitTime = 0
		rawJSON, err := json.MarshalIndent(&sample, "", "    ")
		canonData := fmt.Sprintf("file: %s\n%s", desc.Filter, string(rawJSON))
		require.NoError(t, err)
		fmt.Println(canonData)
		totalCanon = append(totalCanon, canonData)
	}
	sort.Strings(totalCanon)
	canon.SaveJSON(t, strings.Join(totalCanon, "\n"))
}

func TestEstimateTableRowsCount(t *testing.T) {
	testCasePath := "test_csv_large"
	cfg := s3.PrepareCfg(t, "estimate_rows", model.ParsingFormatCSV)
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	cfg.ReadBatchSize = 100_000_0
	cfg.Format.CSVSetting = new(s3.CSVSetting)
	cfg.Format.CSVSetting.BlockSize = 100_000_0
	cfg.Format.CSVSetting.Delimiter = ","
	cfg.Format.CSVSetting.QuoteChar = "\""
	cfg.Format.CSVSetting.EscapeChar = "\\"
	cfg.EventSource.SQS = &s3.SQS{
		QueueName: "test",
	}

	storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	zeroRes, err := storage.EstimateTableRowsCount(*abstract.NewTableID("test", "name"))
	require.NoError(t, err)
	require.Equal(t, uint64(0), zeroRes) // nothing estimated since eventSource configured

	cfg.EventSource.SQS = nil

	res, err := storage.EstimateTableRowsCount(*abstract.NewTableID("test", "name"))
	require.NoError(t, err)
	require.Equal(t, uint64(508060), res) // actual estimated row size
}
