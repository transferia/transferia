//go:build !disable_bigquery_provider

package bigquery

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"google.golang.org/api/googleapi"
)

var _ abstract.Sinker = (*Sinker)(nil)

type Sinker struct {
	cfg       *BigQueryDestination
	logger    log.Logger
	credsPath string
	metrics   *stats.SinkerStats
}

func (s Sinker) Close() error {
	return nil
}

func (s Sinker) Push(items []abstract.ChangeItem) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, s.cfg.ProjectID)
	if err != nil {
		return xerrors.Errorf("bigquery.NewClient: %w", err)
	}
	defer client.Close()
	tbls := abstract.TableMap{}

	for _, row := range items {
		switch row.Kind {
		case abstract.DropTableKind, abstract.TruncateTableKind:
			tableRef := client.Dataset(s.cfg.Dataset).Table(normalizedName(row.TableID()))
			_, err := tableRef.Metadata(ctx)
			if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
				continue
			}
			if err := tableRef.Delete(ctx); err != nil {
				return xerrors.Errorf("unable to delete table: %w", err)
			}
			time.Sleep(time.Second * 30) // well, gcp is piece of human post processed food, see: https://stackoverflow.com/questions/36415265/after-recreating-bigquery-table-streaming-inserts-are-not-working
		default:
			if row.IsRowEvent() {
				tbls[row.TableID()] = abstract.TableInfo{
					EtaRow: 0,
					IsView: false,
					Schema: row.TableSchema,
				}
			}
		}
	}
	for tid, info := range tbls {
		var tSchema bigquery.Schema
		for _, col := range info.Schema.Columns() {
			tSchema = append(tSchema, &bigquery.FieldSchema{
				Name:        col.ColumnName,
				Description: fmt.Sprintf("%s from %s original type %s", col.ColumnName, tid.String(), col.OriginalType),
				Required:    col.Required,
				Type:        inferType(col.DataType),
			})
		}
		metaData := &bigquery.TableMetadata{Schema: tSchema}
		tableRef := client.Dataset(s.cfg.Dataset).Table(normalizedName(tid))
		meta, err := tableRef.Metadata(ctx)
		if err != nil {
			if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
				if err := tableRef.Create(ctx, metaData); err != nil {
					return xerrors.Errorf("unable to create: %s: %w", tid.String(), err)
				}
				continue
			}
			return xerrors.Errorf("unable to fetch table: %s: metadata: %w", tid.String(), err)
		}
		s.logger.Infof("table: %s: meta: %v", normalizedName(tid), meta)
	}

	masterCI := items[0]
	tableRef := client.Dataset(s.cfg.Dataset).Table(normalizedName(masterCI.TableID()))
	var batches [][]abstract.ChangeItem
	var batch []abstract.ChangeItem
	for _, row := range items {
		if !row.IsRowEvent() {
			continue
		}
		if len(batch) >= 1024 {
			batches = append(batches, batch)
			batch = make([]abstract.ChangeItem, 0)
		}
		batch = append(batch, row)
	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}

	return util.ParallelDo(context.Background(), len(batches), 10, func(i int) error {
		return backoff.Retry(func() error {
			items := batches[i]
			st := time.Now()
			var saver []bigquery.ValueSaver
			for _, row := range items {
				if !row.IsRowEvent() {
					continue
				}
				s.metrics.Inflight.Inc()
				if row.Kind == abstract.InsertKind {
					saver = append(saver, ChangeItem{ChangeItem: row})
				}
			}
			if err := tableRef.Inserter().Put(ctx, saver); err != nil {
				return xerrors.Errorf("unable to put rows: %v: %w", len(items), err)
			}
			s.metrics.Table(masterCI.Fqtn(), "rows", len(items))
			s.logger.Infof("batch upload done %v rows in %v", len(items), time.Since(st))
			return nil
		}, backoff.NewExponentialBackOff())
	})
}

func normalizedName(tid abstract.TableID) string {
	if tid.Namespace == "" {
		return tid.Name
	}
	return fmt.Sprintf("%s_%s", tid.Namespace, tid.Name)
}

func inferType(dataType string) bigquery.FieldType {
	return bigquery.FieldType(typesystem.RuleFor(ProviderType).Target[schema.Type(dataType)])
}

func NewSink(cfg *BigQueryDestination, lgr log.Logger, registry metrics.Registry) (*Sinker, error) {
	if err := os.WriteFile("gcpcreds.json", []byte(cfg.Creds), 0o644); err != nil {
		return nil, xerrors.Errorf("unable to write config to FS: %w", err)
	}
	absPath, err := filepath.Abs("gcpcreds.json")
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve abs path for gcpcreds.json: %w", err)
	}
	lgr.Infof("store gcp creds: %s", absPath)
	if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", absPath); err != nil {
		return nil, xerrors.Errorf("unable to set env: %w", err)
	}
	return &Sinker{
		cfg:       cfg,
		logger:    lgr,
		credsPath: absPath,
		metrics:   stats.NewSinkerStats(registry),
	}, nil
}
