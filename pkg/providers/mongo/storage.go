package mongo

import (
	"context"
	"regexp"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.ytsaurus.tech/library/go/core/log"
	goslice "golang.org/x/exp/slices"
)

var (
	emptyFilter            = bson.D{}
	systemCollectionRegexp = regexp.MustCompile(`^system\..*`)
)

const (
	chunkSize     = 5 * 1000
	chunkByteSize = 128 * 1024 * 1024
)

type Storage struct {
	Client            *MongoClientWrapper
	dbFilter          func(dbName string) bool
	IsHomo            bool
	version           *semver.Version
	metrics           *stats.SourceStats
	desiredPartSize   uint64
	preventJSONRepack bool
}

func makeDBFilter(collections []MongoCollection) func(dbName string) bool {
	if len(collections) == 0 {
		return func(string) bool { return true }
	}

	dbNameWhiteList := map[string]struct{}{}
	for _, coll := range collections {
		dbNameWhiteList[coll.DatabaseName] = struct{}{}
	}
	whiteListFilter := func(dbName string) bool {
		_, ok := dbNameWhiteList[dbName]
		return ok
	}
	return whiteListFilter
}

func (s *Storage) Close() {
	defer s.Client.Close(context.Background()) // nolint
}

func (s *Storage) Ping() error {
	return s.Client.Ping(context.Background(), nil)
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return DocumentSchema.Columns, nil
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	st := util.GetTimestampFromContextOrNow(ctx)
	if s.version.GE(MongoVersion4_0) && !s.Client.IsDocDB {
		sess, err := s.Client.StartSession(
			options.Session().
				SetDefaultReadConcern(
					readconcern.Snapshot(),
				),
		)
		if err != nil {
			return xerrors.Errorf("unable to start snapshot session: %w", err)
		}
		defer sess.EndSession(ctx)
		ctx = mongo.NewSessionContext(ctx, sess)
	}

	coll := s.Client.Database(table.Schema).Collection(table.Name)
	tableRowsCount, err := s.Client.Database(table.Schema).Collection(table.Name).EstimatedDocumentCount(ctx)
	if err != nil {
		logger.Log.Warn("Cannot estimate total count", log.Error(err))
	}

	// no cursor timeout is unavailable for lower price tiers in atlas
	// https://www.mongodb.com/docs/atlas/reference/free-shared-limitations/
	findOptions := options.Find().SetMaxTime(606461538 * time.Millisecond) // 1 week

	if table.Offset > 0 {
		findOptions.SetSkip(int64(table.Offset))
		tableRowsCount -= int64(table.Offset)
	}

	filter, err := filterFromTable(table)
	if err != nil {
		return xerrors.Errorf("cannot get mongo filter by table description: %w", err)
	}

	var cursor *mongo.Cursor
	if err := backoff.Retry(func() error {
		cursor, err = coll.Find(ctx, filter, findOptions)

		if err != nil {
			return xerrors.Errorf("Cannot create document cursor: %w", err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)); err != nil {
		return xerrors.Errorf("%w (tried 3 times)", err)
	}
	defer cursor.Close(ctx)

	err = s.readRowsAndPushByChunks(
		ctx,
		cursor,
		st,
		table,
		chunkSize,
		chunkByteSize,
		pusher,
	)
	if err != nil {
		return err
	}

	if err := cursor.Err(); err != nil {
		return xerrors.Errorf("Error while reading documents from cursor: %w", err)
	}
	logger.Log.Info("Sink done uploading table", log.String("fqtn", table.Fqtn()))
	return nil
}

func (s *Storage) TableList(includeTableFilter abstract.IncludeTableList) (abstract.TableMap, error) {
	ctx := context.Background()
	dbList, err := s.makeDBList(ctx, "*")
	if err != nil {
		return nil, xerrors.Errorf("unable to list databases: %w", err)
	}

	tables := make(abstract.TableMap)
	for _, dbName := range dbList {
		dbClient := s.Client.Database(dbName)

		collectionSpecifications, err := dbClient.ListCollectionSpecifications(ctx, CollectionFilter)
		if err != nil {
			// AWS DocumentDB does not support the field 'type' on listCollections()
			if err.Error() != errDocDBTypeUnsupported {
				return nil, xerrors.Errorf("failed to list collection specifications from database: %w", err)
			}
			collectionSpecifications, err = dbClient.ListCollectionSpecifications(context.Background(), bson.D{})
			if err != nil {
				return nil, xerrors.Errorf("failed to list collection specifications from database: %w", err)
			}
		}

		collections, err := filterSystemCollections(collectionSpecifications)
		if err != nil {
			return nil, xerrors.Errorf("failed to filter out system collections from database: %w", err)
		}

		for _, collName := range collections {
			etaRow, err := dbClient.Collection(collName).EstimatedDocumentCount(ctx)
			if err != nil {
				return nil, xerrors.Errorf("unable to estimate: %s: %w", collName, err)
			}

			tableID := abstract.TableID{Namespace: dbName, Name: collName}
			tables[tableID] = abstract.TableInfo{
				EtaRow: uint64(etaRow),
				IsView: false,
				Schema: DocumentSchema.Columns,
			}
		}
	}
	return model.FilteredMap(tables, includeTableFilter), nil
}

func filterSystemCollections(specs []*mongo.CollectionSpecification) ([]string, error) {
	var filteredCollections []string
	for _, spec := range specs {
		if spec.Type == "view" {
			continue
		}
		if systemCollectionRegexp.MatchString(spec.Name) {
			continue
		}
		filteredCollections = append(filteredCollections, spec.Name)
	}

	return filteredCollections, nil
}

func (s *Storage) makeDBList(ctx context.Context, schemaName string) ([]string, error) {
	if schemaName != "*" {
		return []string{schemaName}, nil
	}

	allDBNames, err := s.Client.ListDatabaseNames(ctx, emptyFilter)
	if err != nil {
		return nil, xerrors.Errorf("failed to list all db names from database: %w", err)
	}
	allDBNames = filterSystemDBs(allDBNames, SystemDBs)

	filteredDBNames := make([]string, 0)
	for _, name := range allDBNames {
		if s.dbFilter(name) {
			filteredDBNames = append(filteredDBNames, name)
		}
	}
	return filteredDBNames, nil
}

func filterSystemDBs(allDBNames []string, systemDBs []string) []string {
	return yslices.Filter(allDBNames, func(db string) bool {
		return !goslice.Contains(systemDBs, db)
	})
}

func (s *Storage) LoadSchema() (dbSchema abstract.DBSchema, err error) {
	tableMap, err := s.TableList(nil)
	if err != nil {
		return nil, err
	}

	resultSchema := make(map[abstract.TableID]*abstract.TableSchema)
	for tID := range tableMap {
		resultSchema[tID] = DocumentSchema.Columns
	}

	return resultSchema, nil
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.ExactTableRowsCount(table)
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	numOfDocs, err := s.Client.Database(table.Namespace).Collection(table.Name).CountDocuments(context.Background(), bson.D{})
	if err != nil {
		return 0, err
	}
	return uint64(numOfDocs), nil
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	ctx := context.Background()
	names, err := s.Client.Database(table.Namespace).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return false, err
	}
	for _, name := range names {
		if name == table.Name {
			return true, nil
		}
	}
	return false, nil
}

type StorageOpt func(storage *Storage) *Storage

func WithMetrics(registry metrics.Registry) StorageOpt {
	return func(storage *Storage) *Storage {
		storage.metrics = stats.NewSourceStats(registry)
		return storage
	}
}

func NewStorage(config *MongoStorageParams, opts ...StorageOpt) (*Storage, error) {
	ctx := context.Background()
	client, err := Connect(ctx, config.ConnectionOptions(config.RootCAFiles), nil)
	if err != nil {
		return nil, err
	}

	version, err := GetVersion(ctx, client, config.AuthSource)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve mongo version: %w", err)
	}
	logger.Log.Infof("mongo version: %v", version)
	storage := &Storage{
		Client:            client,
		dbFilter:          makeDBFilter(config.Collections),
		IsHomo:            false,
		version:           version,
		metrics:           stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		desiredPartSize:   config.DesiredPartSize,
		preventJSONRepack: config.PreventJSONRepack,
	}
	for _, opt := range opts {
		storage = opt(storage)
	}

	return storage, nil
}
