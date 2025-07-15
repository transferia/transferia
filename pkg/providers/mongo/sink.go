package mongo

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/stats"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	collectionMovedSuffix = ".renameCollection"
)

func makeErrOperationKindNotSupported(chgItem *abstract.ChangeItem) error {
	return xerrors.Errorf("operation kind not supported: %s", chgItem.Kind)
}

type sinker struct {
	client  *MongoClientWrapper
	logger  log.Logger
	metrics *stats.SinkerStats
	config  *MongoDestination
}

func (s *sinker) Push(input []abstract.ChangeItem) error {
	ctx := context.Background()

	var errMu sync.Mutex
	collectionErrors := map[Namespace]error{}

	setError := func(collID Namespace, err error) {
		errMu.Lock()
		collectionErrors[collID] = err
		errMu.Unlock()
	}

	flushCollection := func(collID Namespace, items []abstract.ChangeItem, wg *sync.WaitGroup) {
		defer wg.Done()

		bulks, err := s.splitItemsToBulkOperations(ctx, collID, items)
		if err != nil {
			setError(collID, err)
			return
		}

		startFlush := time.Now()
		for _, bulk := range bulks {
			err = s.bulkWrite(ctx, collID, bulk)
			if err != nil {
				setError(collID, err)
				return
			}
		}
		s.logger.Infof("Flush collection %v: change items number = %v, bulks number = %v, elapsed = %v", collID.GetFullName(), len(items), len(bulks), time.Since(startFlush))
	}

	s.metrics.Inflight.Add(int64(len(input)))
	startPush := time.Now()

	if s.config.Database != "" {
		for i := range input {
			input[i].Schema = s.config.Database
		}
	}

	for i := 0; i < len(input); {
		ddl, collections, err := s.splitByDDLAndCollection(input, &i)
		if err != nil {
			return xerrors.Errorf("unable to split ddl and dml items: %w", err)
		}
		if (ddl == nil && collections == nil) || (ddl != nil && collections != nil) {
			return xerrors.New("logic error on change items process")
		}
		if ddl != nil {
			err = s.processDDL(ctx, ddl)
			if err != nil {
				return xerrors.Errorf("unable to process ddl: %w", err)
			}
		}
		if collections != nil {
			var wg sync.WaitGroup
			for collID, items := range collections {
				wg.Add(1)
				go flushCollection(collID, items, &wg)
			}
			wg.Wait()
			if len(collectionErrors) > 0 {
				return joinErrors(collectionErrors)
			}
		}
	}

	elapsed := time.Since(startPush)
	s.metrics.Elapsed.RecordDuration(elapsed)
	s.metrics.Wal.Add(int64(len(input)))
	s.logger.Infof("Push %d documents in %v", len(input), elapsed)
	return nil
}

func (s *sinker) isDDLItem(item *abstract.ChangeItem) bool {
	switch item.Kind {
	case
		abstract.InitShardedTableLoad,
		abstract.InitTableLoad,
		abstract.DoneTableLoad,
		abstract.DoneShardedTableLoad,
		abstract.DropTableKind,
		abstract.TruncateTableKind,
		abstract.SynchronizeKind,
		abstract.MongoCreateKind,
		abstract.MongoDropKind,
		abstract.MongoRenameKind,
		abstract.MongoDropDatabaseKind:
		return true
	default:
		return false
	}
}

func (s *sinker) isDataItem(item *abstract.ChangeItem) bool {
	switch item.Kind {
	case abstract.InsertKind, abstract.UpdateKind, abstract.DeleteKind, abstract.MongoUpdateDocumentKind:
		return true
	default:
		return false
	}
}

func (s *sinker) mongoCollection(tid abstract.TableID) Namespace {
	namespace := Namespace{
		Database:   tid.Namespace,
		Collection: tid.Name,
	}
	if namespace.Database == "" {
		namespace.Database = s.config.Database
	}
	return namespace
}

func (s *sinker) splitByDDLAndCollection(items []abstract.ChangeItem, index *int) (*abstract.ChangeItem, map[Namespace][]abstract.ChangeItem, error) {
	if *index >= len(items) {
		return nil, nil, xerrors.Errorf("invalid index %v, range is [0, %v)", *index, len(items))
	}
	if s.isDDLItem(&items[*index]) {
		result := &items[*index]
		*index++
		return result, nil, nil
	}
	result := map[Namespace][]abstract.ChangeItem{}
	for ; *index < len(items); *index++ {
		item := &items[*index]
		if s.isDDLItem(item) {
			return nil, result, nil
		} else if s.isDataItem(item) {
			namespace := s.mongoCollection(item.TableID())
			item.Schema = namespace.Database // in any case schema bounded to namespace database
			result[namespace] = append(result[namespace], *item)
		} else {
			return nil, nil, xerrors.Errorf("operation not supported: %s", item.Kind)
		}
	}
	return nil, result, nil
}

func (s *sinker) processDDL(ctx context.Context, item *abstract.ChangeItem) error {
	switch item.Kind {
	case abstract.InitShardedTableLoad, abstract.DoneShardedTableLoad, abstract.DoneTableLoad, abstract.SynchronizeKind:
		// not needed for now
	case abstract.InitTableLoad:
		if err := s.createCollectionIfNotExists(ctx, s.mongoCollection(item.TableID())); err != nil {
			return xerrors.Errorf("unable to create collection on init table load: %w", err)
		}
	case abstract.DropTableKind:
		if s.config.Cleanup != model.Drop {
			s.logger.Infof("Skipped dropping collection '%v.%v' due cleanup policy", item.Schema, item.Table)
			return nil
		}
		if err := s.dropCollection(ctx, s.mongoCollection(item.TableID())); err != nil {
			return xerrors.Errorf("unable to drop collection: %w", err)
		}
	case abstract.TruncateTableKind:
		if s.config.Cleanup != model.Truncate {
			s.logger.Infof("Skipped truncating collection '%v.%v' due cleanup policy", item.Schema, item.Table)
			return nil
		}
		if err := s.truncateCollection(ctx, s.mongoCollection(item.TableID())); err != nil {
			return xerrors.Errorf("unable to truncate collection: %w", err)
		}
	case abstract.MongoCreateKind:
		if err := s.createCollection(ctx, item); err != nil {
			return xerrors.Errorf("unable to create collection: %w", err)
		}
	case abstract.MongoDropKind:
		if err := s.dropCollection(ctx, s.mongoCollection(item.TableID())); err != nil {
			return xerrors.Errorf("unable to drop collection: %w", err)
		}
	case abstract.MongoRenameKind:
		if err := s.renameCollection(ctx, item); err != nil {
			return xerrors.Errorf("unable to rename collection: %w", err)
		}
	case abstract.MongoDropDatabaseKind:
		if err := s.dropDatabase(item); err != nil {
			return xerrors.Errorf("unable to drop database: %w", err)
		}
	default:
		return xerrors.Errorf("operation not supported: %s", item.Kind)
	}
	return nil
}

func joinErrors(errors map[Namespace]error) error {
	joined := ""
	for collID, err := range errors {
		if len(joined) > 0 {
			joined += "\n"
		}
		joined += fmt.Sprintf("\t%v.%v: %v", collID.Database, collID.Collection, err)
	}
	return xerrors.Errorf("Failed write into some collections:\n%v", joined)
}

func (s *sinker) createCollectionIfNotExists(ctx context.Context, item Namespace) error {
	db := s.client.Database(item.Database)

	collections, err := db.ListCollectionNames(ctx, bson.D{
		{
			Key:   "name",
			Value: item.Collection,
		},
	})
	if err != nil {
		return xerrors.Errorf("Can't list collections: %w", err)
	}

	if len(collections) > 0 {
		s.logger.Infof("Skipped creating collection '%s.%s' - already exists", item.Database, item.Collection)
		return nil
	}

	s.logger.Infof("Create collection '%s.%s'", item.Database, item.Collection)
	return db.CreateCollection(ctx, item.Collection)
}

func (s *sinker) createCollection(ctx context.Context, item *abstract.ChangeItem) error {
	db := s.client.Database(item.Schema)
	s.logger.Infof("Create collection '%s.%s'", item.Schema, item.Table)
	return db.CreateCollection(ctx, item.Table)
}

func (s *sinker) dropCollection(ctx context.Context, collection Namespace) error {
	db := s.client.Database(collection.Database)
	coll := db.Collection(collection.Collection)
	s.logger.Infof("Drop collection '%s.%s'", collection.Database, collection.Collection)
	return coll.Drop(ctx)
}

func (s *sinker) truncateCollection(ctx context.Context, collection Namespace) error {
	db := s.client.Database(collection.Database)
	coll := db.Collection(collection.Collection)
	deleteResult, err := coll.DeleteMany(ctx, bson.D{})
	if err != nil {
		return err
	}
	s.logger.Infof("Removed %d documents from collection '%s.%s'", deleteResult.DeletedCount, collection.Database, collection.Collection)
	return nil
}

func (s *sinker) getRenameItemNamespaces(item *abstract.ChangeItem) (Namespace, Namespace, error) {
	var fromNamespace Namespace
	var toNamespace Namespace

	if len(item.OldKeys.KeyValues) != 1 {
		return fromNamespace, toNamespace, xerrors.New("invalid mongo rename change item")
	}
	fromNamespace, ok := item.OldKeys.KeyValues[0].(Namespace)
	if !ok {
		return fromNamespace, toNamespace, xerrors.New("invalid mongo rename change item")
	}

	if len(item.ColumnValues) != 1 {
		return fromNamespace, toNamespace, xerrors.New("invalid mongo rename change item")
	}
	toNamespace, ok = item.ColumnValues[0].(Namespace)
	if !ok {
		return fromNamespace, toNamespace, xerrors.New("invalid mongo rename change item")
	}

	return fromNamespace, toNamespace, nil
}

func (s *sinker) renameCollection(ctx context.Context, item *abstract.ChangeItem) error {
	fromNamespace, toNamespace, err := s.getRenameItemNamespaces(item)
	if err != nil {
		return xerrors.Errorf("unable to get collection renaming: %w", err)
	}
	fromNamespaceName := fromNamespace.GetFullName()
	toNamespaceName := toNamespace.GetFullName()

	if strings.Contains(fromNamespace.Collection, collectionMovedSuffix) || strings.Contains(toNamespace.Collection, collectionMovedSuffix) {
		s.logger.Warnf("Collection move between DBs detected: can't rename collection '%v' to '%v'", fromNamespaceName, toNamespaceName)
		return nil
	}

	db := s.client.Database("admin") // rename is admin operation
	result := db.RunCommand(ctx,
		bson.D{{Key: "renameCollection", Value: fromNamespaceName}, {Key: "to", Value: toNamespaceName}})
	err = result.Err()
	if err != nil && err != mongo.ErrNoDocuments {
		return xerrors.Errorf("unable to rename collection: %w", err)
	}
	s.logger.Infof("Rename collection '%v' to '%v'", fromNamespaceName, toNamespaceName)
	return nil
}

func (s *sinker) dropDatabase(item *abstract.ChangeItem) error {
	s.logger.Warnf("Drop database '%v' skipped", item.Schema)
	return nil
}

func (s *sinker) Close() error {
	return s.client.Close(context.Background())
}

func NewSinker(lgr log.Logger, dst *MongoDestination, mtrcs metrics.Registry) (abstract.Sinker, error) {
	opts := dst.ConnectionOptions(dst.RootCAFiles)
	ctx := context.Background()
	client, err := Connect(ctx, opts, lgr)
	if err != nil {
		return nil, err
	}
	return &sinker{
		client:  client,
		logger:  lgr,
		metrics: stats.NewSinkerStats(mtrcs),
		config:  dst,
	}, nil
}
