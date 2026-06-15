package ydb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/castx"
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

const (
	// see: https://st.yandex-team.ru/DTSUPPORT-2428
	// 	some old DBs can have v0 query syntax enabled by default, so we must enforce v1 syntax
	// 	more details here: https://clubs.at.yandex-team.ru/ydb/336
	ydbV1 = "--!syntax_v1\n"
)

func execQuery(ctx context.Context, ydbClient *ydb_go_sdk.Driver, query string) error {
	err := ydbClient.Table().Do(ctx, func(ctx context.Context, s ydb_table.Session) error {
		err := s.ExecuteSchemeQuery(ctx, query)
		if err != nil {
			return xerrors.Errorf("failed to execute changefeed query '%s': %w", query, err)
		}
		return nil
	}, ydb_table.WithIdempotent())
	if err != nil {
		return xerrors.Errorf("failed to modify changefeed: %w", err)
	}
	return nil
}

func dropChangeFeedIfExistsOneTable(ctx context.Context, ydbClient *ydb_go_sdk.Driver, tablePath, transferID string) (deleted bool, err error) {
	query := fmt.Sprintf(ydbV1+"ALTER TABLE `%s` DROP CHANGEFEED %s", tablePath, transferID)
	err = execQuery(ctx, ydbClient, query)
	if err != nil {
		if strings.Contains(err.Error(), "path hasn't been resolved, nearest resolved path") {
			// no topics was deleted, but error should be empty if no such topic exist
			return false, nil
		}
		return false, xerrors.Errorf("unable to drop changefeed, err: %w", err)
	}
	return true, nil
}

func createChangeFeedOneTable(ctx context.Context, ydbClient *ydb_go_sdk.Driver, tablePath, transferID string, cfg *YdbSource) error {
	autoPartitioningStr := ", TOPIC_AUTO_PARTITIONING = 'ENABLED'"
	if err := createChangeFeedWithAutoPartitioning(ctx, ydbClient, autoPartitioningStr, tablePath, transferID, cfg); err == nil {
		logger.Log.Infof("changefeed created with auto partitioning for table %s", tablePath)
		return nil
	} else {
		logger.Log.Infof("unable to create changefeed with auto partitioning for table %s err: %s", tablePath, err.Error())
	}
	logger.Log.Infof("trying to create changefeed without auto partitioning for table %s", tablePath)
	return createChangeFeedWithAutoPartitioning(ctx, ydbClient, "", tablePath, transferID, cfg)
}

func createChangeFeedWithAutoPartitioning(ctx context.Context, ydbClient *ydb_go_sdk.Driver, autoPartitioningStr string, tablePath, transferID string, cfg *YdbSource) error {
	queryParams := fmt.Sprintf("FORMAT = 'JSON', MODE = '%s'%s", string(cfg.ChangeFeedMode), autoPartitioningStr)
	if period := cfg.ChangeFeedRetentionPeriod; period != nil {
		asIso, err := castx.DurationToIso8601(*period)
		if err != nil {
			return xerrors.Errorf("unable to represent retention period as ISO 8601: %w", err)
		}
		queryParams += fmt.Sprintf(", RETENTION_PERIOD = Interval('%s')", asIso)
	}
	query := fmt.Sprintf(ydbV1+"ALTER TABLE `%s` ADD CHANGEFEED %s WITH (%s)", tablePath, transferID, queryParams)
	err := execQuery(ctx, ydbClient, query)
	if err != nil {
		return xerrors.Errorf("unable to add changefeed, err: %w", err)
	}
	topicPath := makeChangeFeedPath(tablePath, transferID)
	err = ydbClient.Topic().Alter(
		ctx,
		topicPath,
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{Name: dataTransferConsumerName}),
	)
	if err != nil {
		return xerrors.Errorf("unable to add consumer, err: %w", err)
	}
	return nil
}

// checkChangeFeedConsumerOnline
// with this method we identify changefeed is active if our system consumer is attached to it as well
func checkChangeFeedConsumerOnline(ctx context.Context, ydbClient *ydb_go_sdk.Driver, tablePath, transferID string) (bool, error) {
	topicPath := makeChangeFeedPath(tablePath, transferID)
	descr, err := ydbClient.Topic().Describe(ctx, topicPath)
	if err != nil {
		return false, err
	}
	for _, consumer := range descr.Consumers {
		if consumer.Name == dataTransferConsumerName {
			return true, nil
		}
	}
	return false, nil
}

func makeChangeFeedPath(tablePath, feedName string) string {
	return path.Join(tablePath, feedName)
}

func makeTablePathFromTopicPath(topicPath, feedName, database string) string {
	result := strings.TrimSuffix(topicPath, "/"+feedName)
	if database[0] != '/' {
		database = "/" + database
	}
	result = strings.TrimPrefix(result, database)
	result = strings.TrimPrefix(result, "/")
	return result
}

func CreateChangeFeed(ctx context.Context, cfg *YdbSource, transferID string, ydbClient *ydb_go_sdk.Driver) error {
	if cfg.ChangeFeedCustomName != "" {
		return nil // User already created changefeed and specified its name.
	}

	createCtx, cancel := context.WithTimeout(ctx, time.Minute*3)
	defer cancel()
	for _, tablePath := range cfg.Tables {
		if err := createChangeFeedOneTable(createCtx, ydbClient, tablePath, transferID, cfg); err != nil {
			return xerrors.Errorf("unable to create changeFeed for table %s, err: %w", tablePath, err)
		}
	}
	return nil
}

func CreateChangeFeedIfNotExists(ctx context.Context, cfg *YdbSource, transferID string, ydbClient *ydb_go_sdk.Driver) error {
	if cfg.ChangeFeedCustomName != "" {
		return nil // User already created changefeed and specified its name.
	}

	createCtx, cancel := context.WithTimeout(ctx, time.Minute*3)
	defer cancel()
	for _, tablePath := range cfg.Tables {
		isOnline, err := checkChangeFeedConsumerOnline(createCtx, ydbClient, tablePath, transferID)
		if err != nil {
			return xerrors.Errorf("cannot check feed consumer online: %w", err)
		}
		if isOnline {
			continue
		}
		err = createChangeFeedOneTable(createCtx, ydbClient, tablePath, transferID, cfg)
		if err != nil {
			return xerrors.Errorf("unable to create changeFeed for table %s, err: %w", tablePath, err)
		}
	}
	return nil
}

func DropChangeFeed(ctx context.Context, cfg *YdbSource, transferID string, ydbClient *ydb_go_sdk.Driver) error {
	if cfg.ChangeFeedCustomName != "" {
		return nil // Don't drop changefeed that was manually created by user.
	}

	dropCtx, cancel := context.WithTimeout(ctx, time.Minute*3)
	defer cancel()
	var mErr []error
	for _, tablePath := range cfg.Tables {
		_, err := dropChangeFeedIfExistsOneTable(dropCtx, ydbClient, tablePath, transferID)
		if err != nil {
			mErr = append(mErr, xerrors.Errorf("unable to drop changeFeed for table %s, err: %w", tablePath, err))
		}
	}
	return errors.Join(mErr...)
}

func CommittedEndOffsetsForCustomFeed(ctx context.Context, cfg *YdbSource, ydbClient *ydb_go_sdk.Driver) error {
	if cfg.ChangeFeedCustomName == "" {
		return nil
	}

	commitCtx, cancel := context.WithTimeout(ctx, time.Minute*3)
	defer cancel()

	consumer := cfg.customConsumerOrDefault()
	for _, table := range cfg.Tables {
		feedFullPath := makeChangeFeedPath(table, cfg.ChangeFeedCustomName)
		consumerDescription, err := ydbClient.Topic().DescribeTopicConsumer(commitCtx, feedFullPath, consumer)
		if err != nil {
			return xerrors.Errorf("unable to describe topic consumer, err: %w", err)
		}

		for _, partition := range consumerDescription.Partitions {
			endOffset := partition.PartitionStats.PartitionsOffset.End
			if err = ydbClient.Topic().CommitOffset(commitCtx, feedFullPath, partition.PartitionID, consumer, endOffset); err != nil {
				return xerrors.Errorf("unable to commit offset for partition %d, err: %w", partition.PartitionID, err)
			}
		}
	}

	return nil
}
