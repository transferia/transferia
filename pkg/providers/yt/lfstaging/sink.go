//go:build !disable_yt_provider

package lfstaging

import (
	"context"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	ytcommon "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/guid"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/xerrors"
)

type sink struct {
	config             *sinkConfig
	logger             log.Logger
	ytClient           yt.Client
	aggregator         *tableAggregator
	intermediateWriter *intermediateWriter
}

type sinkConfig struct {
	cluster           string
	topic             string
	tmpPath           ypath.Path
	stagingPath       ypath.Path
	jobIndex          int
	ytAccount         string
	ytPool            string
	aggregationPeriod time.Duration

	usePersistentIntermediateTables bool
	useNewMetadataFlow              bool

	secondsPerTmpTable int64
	bytesPerTmpTable   int64
}

func (s *sink) createDirectories() error {
	return yt.ExecTx(context.Background(), s.ytClient, func(ctx context.Context, tx yt.Tx) error {
		createDir := func(path ypath.Path) error {
			newDirAttrs := map[string]interface{}{}

			if s.config.ytAccount != "" {
				newDirAttrs["account"] = s.config.ytAccount
			}

			_, err := tx.CreateNode(context.Background(), path, yt.NodeMap, &yt.CreateNodeOptions{
				Recursive:      true,
				IgnoreExisting: true,
				Attributes:     newDirAttrs,
			})
			return err
		}

		if err := createDir(s.config.tmpPath); err != nil {
			return xerrors.Errorf("Cannot create dir '%v': %w", s.config.tmpPath.String(), err)
		}

		if err := createDir(s.config.stagingPath); err != nil {
			return xerrors.Errorf("Cannot create dir '%v': %w", s.config.stagingPath.String(), err)
		}

		return nil
	}, nil)
}

func (s *sink) Push(changeItems []abstract.ChangeItem) error {
	// don't create a lot of empty tables
	if len(changeItems) == 0 {
		return nil
	}

	if s.config.usePersistentIntermediateTables {
		err := s.intermediateWriter.Write(changeItems)
		if err != nil {
			return xerrors.Errorf("cannot push using intermediate writer: %w", err)
		}
	} else {
		err := yt.ExecTx(context.Background(), s.ytClient, func(_ context.Context, tx yt.Tx) error {
			writer, path, err := s.getUniqueWriter(tx)
			if err != nil {
				return xerrors.Errorf("Cannot create writer: %w", err)
			}

			metadata := newLogbrokerMetadata()

			for _, changeItem := range changeItems {
				row, err := intermediateRowFromChangeItem(changeItem)
				if err != nil {
					return xerrors.Errorf("Cannot convert changeitem to intermediate row: %w", err)
				}

				metadata.AddIntermediateRow(row)

				if s.config.useNewMetadataFlow {
					outputRow := lfStagingRowFromIntermediate(row)
					if err = writer.Write(outputRow); err != nil {
						return xerrors.Errorf("Cannot write into the logfeller writer: %w", err)
					}
				} else {
					if err = writer.Write(row); err != nil {
						return xerrors.Errorf("Cannot write into the intermediate writer: %w", err)
					}
				}
			}

			err = metadata.saveIntoTableAttr(tx, path)
			if err != nil {
				return xerrors.Errorf("Could not set new table attrs")
			}

			if err = writer.Commit(); err != nil {
				return xerrors.Errorf("Cannot commit the writer: %w", err)
			}

			return nil
		}, nil)
		if err != nil {
			return xerrors.Errorf("cannot push using new table: %w", err)
		}
	}
	return nil
}

func (s *sink) getUniqueWriter(tx yt.Tx) (yt.TableWriter, ypath.Path, error) {
	var schema ytschema.Schema
	var err error

	if s.config.useNewMetadataFlow {
		schema, err = lfStagingRowSchema()
		if err != nil {
			return nil, "", xerrors.Errorf("Cannot infer logfeller table schema: %w", err)
		}
	} else {
		schema, err = intermediateRowSchema()
		if err != nil {
			return nil, "", xerrors.Errorf("Cannot infer intermediate table schema: %w", err)
		}
	}

	return s.getUniqueWriterWithSchema(tx, schema)
}

func (s *sink) getUniqueWriterWithSchema(tx yt.Tx, schema ytschema.Schema) (yt.TableWriter, ypath.Path, error) {
	name := guid.New().String()
	path := s.config.tmpPath.Child(name)

	newTableAttrs := map[string]interface{}{}
	if s.config.ytAccount != "" {
		newTableAttrs["account"] = s.config.ytAccount
	}

	_, err := yt.CreateTable(
		context.Background(),
		tx,
		path,
		yt.WithSchema(schema),
		yt.WithAttributes(newTableAttrs),
	)
	if err != nil {
		return nil, "", xerrors.Errorf("Cannot create unique tmp table: %w", err)
	}

	rawWriter, err := tx.WriteTable(
		context.Background(),
		s.config.tmpPath.Child(name),
		nil,
	)
	if err != nil {
		return nil, "", xerrors.Errorf("Cannot create tmp table writer: %w", err)
	}
	return rawWriter, path, nil
}

func (s sink) Close() error {
	return nil
}

func NewSinker(
	cfg *ytcommon.LfStagingDestination,
	jobIndex int,
	transfer *model.Transfer,
	logger log.Logger,
) (abstract.Sinker, error) {
	ytClient, err := ytclient.NewYtClientWrapper(ytclient.HTTP, logger, &yt.Config{
		Proxy:                 cfg.Cluster,
		Token:                 cfg.YtToken,
		AllowRequestsFromJob:  true,
		DisableProxyDiscovery: false,
	})
	if err != nil {
		return nil, xerrors.Errorf("Cannot create yt client: %w", err)
	}

	if cfg.UseNewMetadataFlow && cfg.Topic == "" {
		return nil, xerrors.New("don't use an empty topic with UseNetMetadataFlow")
	}

	config := &sinkConfig{
		cluster:                         cfg.Cluster,
		topic:                           cfg.Topic,
		tmpPath:                         ypath.Path(cfg.TmpBasePath),
		stagingPath:                     ypath.Path(cfg.LogfellerHomePath).Child("staging-area"),
		ytAccount:                       cfg.YtAccount,
		ytPool:                          cfg.MergeYtPool,
		jobIndex:                        jobIndex,
		aggregationPeriod:               cfg.AggregationPeriod,
		useNewMetadataFlow:              cfg.UseNewMetadataFlow,
		usePersistentIntermediateTables: cfg.UsePersistentIntermediateTables,
		secondsPerTmpTable:              cfg.SecondsPerTmpTable,
		bytesPerTmpTable:                cfg.BytesPerTmpTable,
	}

	s := &sink{
		config:   config,
		ytClient: ytClient,
		logger:   logger,
		aggregator: newTableAggregator(
			config,
			ytClient,
			logger,
		),
		intermediateWriter: nil,
	}

	if err = s.createDirectories(); err != nil {
		return s, xerrors.Errorf("Cannot create required directories: %w", err)
	}

	if jobIndex == 0 {
		s.logger.Info("Job index is 0 - starting aggregator")
		go s.aggregator.Start()
	} else {
		s.logger.Info("Job index not 0 - not starting aggregator")
	}

	if config.usePersistentIntermediateTables {
		s.intermediateWriter, err = newIntermediateWriter(config, ytClient, logger)
		if err != nil {
			return nil, xerrors.Errorf("cannot create intermediate writer: %w", err)
		}
	}

	return s, nil
}
