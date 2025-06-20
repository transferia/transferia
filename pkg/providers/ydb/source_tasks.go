//go:build !disable_ydb_provider

package ydb

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/castx"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

const (
	dataTransferConsumerName = "__data_transfer_consumer"
	// see: https://st.yandex-team.ru/DTSUPPORT-2428
	// 	some old DBs can have v0 query syntax enabled by default, so we must enforce v1 syntax
	// 	more details here: https://clubs.at.yandex-team.ru/ydb/336
	ydbV1 = "--!syntax_v1\n"
)

func execQuery(ctx context.Context, ydbClient *ydb.Driver, query string) error {
	err := ydbClient.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		err := s.ExecuteSchemeQuery(ctx, query)
		if err != nil {
			return xerrors.Errorf("failed to execute changefeed query '%s': %w", query, err)
		}
		return nil
	}, table.WithIdempotent())
	if err != nil {
		return xerrors.Errorf("failed to modify changefeed: %w", err)
	}
	return nil
}

func dropChangeFeedIfExistsOneTable(ctx context.Context, ydbClient *ydb.Driver, tablePath, transferID string) (deleted bool, err error) {
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

func createChangeFeedOneTable(ctx context.Context, ydbClient *ydb.Driver, tablePath, transferID string, cfg *YdbSource) error {
	queryParams := fmt.Sprintf("FORMAT = 'JSON', MODE = '%s'", string(cfg.ChangeFeedMode))

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
func checkChangeFeedConsumerOnline(ctx context.Context, ydbClient *ydb.Driver, tablePath, transferID string) (bool, error) {
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

func CreateChangeFeed(cfg *YdbSource, transferID string) error {
	if cfg.ChangeFeedCustomName != "" {
		return nil // User already created changefeed and specified its name.
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	ydbClient, err := newYDBSourceDriver(ctx, cfg)
	if err != nil {
		return xerrors.Errorf("unable to create ydb, err: %w", err)
	}

	for _, tablePath := range cfg.Tables {
		err = createChangeFeedOneTable(ctx, ydbClient, tablePath, transferID, cfg)
		if err != nil {
			return xerrors.Errorf("unable to create changeFeed for table %s, err: %w", tablePath, err)
		}
	}
	return nil
}

func CreateChangeFeedIfNotExists(cfg *YdbSource, transferID string) error {
	if cfg.ChangeFeedCustomName != "" {
		return nil // User already created changefeed and specified its name.
	}

	clientCtx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	ydbClient, err := newYDBSourceDriver(clientCtx, cfg)
	if err != nil {
		return xerrors.Errorf("unable to create ydb, err: %w", err)
	}

	for _, tablePath := range cfg.Tables {
		isOnline, err := checkChangeFeedConsumerOnline(clientCtx, ydbClient, tablePath, transferID)
		if err != nil {
			return xerrors.Errorf("cannot check feed consumer online: %w", err)
		}
		if isOnline {
			continue
		}
		err = createChangeFeedOneTable(clientCtx, ydbClient, tablePath, transferID, cfg)
		if err != nil {
			return xerrors.Errorf("unable to create changeFeed for table %s, err: %w", tablePath, err)
		}
	}
	return nil
}

func DropChangeFeed(cfg *YdbSource, transferID string) error {
	if cfg.ChangeFeedCustomName != "" {
		return nil // Don't drop changefeed that was manually created by user.
	}

	clientCtx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	ydbClient, err := newYDBSourceDriver(clientCtx, cfg)
	if err != nil {
		return xerrors.Errorf("unable to create ydb, err: %w", err)
	}

	var mErr util.Errors
	for _, tablePath := range cfg.Tables {
		_, err := dropChangeFeedIfExistsOneTable(clientCtx, ydbClient, tablePath, transferID)
		if err != nil {
			mErr = append(mErr, xerrors.Errorf("unable to drop changeFeed for table %s, err: %w", tablePath, err))
		}
	}
	if !mErr.Empty() {
		return mErr
	}
	return nil
}
