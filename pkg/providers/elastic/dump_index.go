//go:build !disable_elastic_provider

package elastic

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	sink_factory "github.com/transferia/transferia/pkg/sink"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

type IsElasticLikeSource interface {
	ToElasticSearchSource() (*ElasticSearchSource, ServerType)
}

type IsElasticLikeDestination interface {
	ToElasticSearchDestination() (*ElasticSearchDestination, ServerType)
}

// sourceHomoElasticSearch returns a non-nil object only for homogenous OpenSearch / ElasticSearch transfers
func srcDstHomoElasticSearch(transfer *model.Transfer) (*ElasticSearchSource, ServerType) {
	src, srcIsElasticLike := transfer.Src.(IsElasticLikeSource)
	_, dstIsElasticLike := transfer.Dst.(IsElasticLikeDestination)
	if srcIsElasticLike && dstIsElasticLike {
		return src.ToElasticSearchSource()
	}
	return nil, 0
}

func DumpIndexInfo(transfer *model.Transfer, logger log.Logger, mRegistry metrics.Registry) error {
	src, serverType := srcDstHomoElasticSearch(transfer)
	if src == nil {
		return nil
	}
	if !src.DumpIndexWithMapping {
		return nil
	}
	logger.Info("index info dumping")
	storage, err := NewStorage(src, logger, mRegistry, serverType)
	if err != nil {
		return xerrors.Errorf("unable to create storage: %w", err)
	}
	tables, err := storage.TableList(transfer)
	if err != nil {
		return xerrors.Errorf("unable to get source indexes list: %w", err)
	}
	logger.Infof("got %v indexes", len(tables))

	for tableName := range tables {
		indexParams, err := storage.getRawIndexParams(tableName.Name)
		if err != nil {
			return xerrors.Errorf("unable to extract params for index %q: %w", tableName.Name, err)
		}
		if err := applyDump(tableName.Name, indexParams, transfer, mRegistry); err != nil {
			return xerrors.Errorf("unable to apply index dump for %q: %w. Raw index params: %v", tableName, err, indexParams)
		}
	}
	return nil
}

func WaitForIndexToExist(client *elasticsearch.Client, indexName string, timeout time.Duration) error {
	time.Sleep(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return backoff.Retry(func() error {
		_, err := getResponseBody(client.Indices.Exists([]string{indexName}))
		if err != nil {
			return xerrors.Errorf("Failed to check the index for existence: %w", err)
		}
		return nil
	},
		backoff.WithContext(backoff.NewExponentialBackOff(), ctx),
	)
}

func applyDump(indexName string, indexParams []byte, transfer *model.Transfer, registry metrics.Registry) error {
	sink, err := sink_factory.MakeAsyncSink(transfer, logger.Log, registry, coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return err
	}
	defer sink.Close()
	logger.Log.Infof("Try to apply an index dump for %q", indexName)
	if err := <-sink.AsyncPush([]abstract.ChangeItem{{
		ID:           0,
		LSN:          0,
		CommitTime:   uint64(time.Now().UnixNano()),
		Counter:      0,
		Kind:         abstract.ElasticsearchDumpIndexKind,
		Schema:       "",
		Table:        indexName,
		PartID:       "",
		ColumnNames:  nil,
		ColumnValues: []interface{}{string(indexParams)},
		TableSchema:  nil,
		OldKeys: abstract.OldKeysType{
			KeyNames:  nil,
			KeyTypes:  nil,
			KeyValues: nil,
		},
		TxID:  "",
		Query: "",
		Size: abstract.EventSize{
			Read:   0,
			Values: 0,
		},
	}}); err != nil {
		logger.Log.Error(
			fmt.Sprintf("Unable to apply index %q dump", indexName),
			log.Error(err))
		return xerrors.Errorf("Unable to apply index %q dump: %w", indexName, err)
	}
	return nil
}

func DeleteSystemFieldsFromIndexParams(params map[string]interface{}) {
	deleteMask := set.New([]string{
		"settings.index.provided_name",
		"settings.index.creation_date",
		"settings.index.number_of_replicas",
		"settings.index.uuid",
		"settings.index.version",
	}...)

	tmp := params
	deleteMask.Range(func(path string) {
		splitPath := strings.Split(path, ".")
		for i, s := range splitPath {
			if i == len(splitPath)-1 {
				delete(tmp, s)
			}
			nextPathField, exists := tmp[s]
			if !exists {
				break
			}
			tmp = nextPathField.(map[string]interface{})

		}
		tmp = params
	})
}
