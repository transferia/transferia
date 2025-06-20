//go:build !disable_mongo_provider

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
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	ChangeStreamFatalErrorCode  = 280
	ChangeStreamHistoryLostCode = 286
)

var (
	fatalCodes = map[int32]bool{
		ChangeStreamHistoryLostCode: true,
		ChangeStreamFatalErrorCode:  true,
	}
	readTimeout = 60 * time.Minute
)

func isFatalMongoCode(err error) bool {
	mErr := new(mongo.CommandError)
	if !xerrors.As(err, &mErr) && !xerrors.As(err, mErr) {
		return false
	}
	return fatalCodes[mErr.Code]
}

type mongoSource struct {
	logger      log.Logger
	metrics     *stats.SourceStats
	cp          coordinator.Coordinator
	config      *MongoSource
	filter      MongoCollectionFilter
	ctx         context.Context
	cancel      func()
	doneChannel chan error

	onceShutdown         sync.Once
	sink                 abstract.AsyncSink
	client               *MongoClientWrapper
	readWg               sync.WaitGroup
	writeWg              sync.WaitGroup
	changeEvents         chan *puChangeEvent
	readTimer            *time.Timer
	transferID           string
	filterOplogWithRegex bool
}

func (s *mongoSource) Run(sink abstract.AsyncSink) error {
	s.sink = sink
	s.changeEvents = make(chan *puChangeEvent)

	var err error
	s.client, err = Connect(s.ctx, s.config.ConnectionOptions(s.config.RootCAFiles), s.logger)
	if err != nil {
		return xerrors.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer func() {
		_ = s.client.Close(context.Background()) // TODO
	}()

	changeStreams, err := s.openChangeStreams()
	if err != nil {
		if abstract.IsFatal(err) {
			s.metrics.Fatal.Inc()
		}
		return xerrors.Errorf("Failed to open change stream: %w", err)
	}
	if len(changeStreams) == 0 {
		// drop replication with fatal error (nothing to replicate)
		return abstract.NewFatalError(xerrors.New("Empty list of watchers. Nothing to replicate."))
	}

	s.readWg.Add(len(changeStreams))
	for parallelizationUnit, watcher := range changeStreams {
		pu := parallelizationUnit
		// prevents error when blocking on channel and specifies parallelization unit
		eventPusher := func(ctx context.Context, event *changeEvent) error {
			if event == nil {
				return xerrors.Errorf("nil keyEvent in source change keyEvent pusher")
			}
			select {
			case <-ctx.Done():
				s.logger.Warn("context done in source change keyEvent pusher", log.Error(ctx.Err()))
				return ctx.Err()
			case s.changeEvents <- &puChangeEvent{
				changeEvent:         *event,
				parallelizationUnit: pu,
			}:
			}
			return nil
		}

		go func(parallelizationUnit ParallelizationUnit, watcher ChangeStreamWatcher, pusher changeEventPusher) {
			defer s.readWg.Done()
			currentWatcher := watcher
			// watchers
			defer func(watcher *ChangeStreamWatcher) {
				if currentWatcher != nil {
					currentWatcher.Close(s.ctx)
				}
			}(&currentWatcher)
			for currentWatcher != nil {
				err := currentWatcher.Watch(s.ctx, pusher)
				// goroutine should die on her own when context finished
				// this is an exception which will never be processed by watcher fallback
				if xerrors.Is(err, context.Canceled) {
					return
				}
				if err == nil {
					return
				}

				fatalErrorSubstrings := []string{
					"resume point may no longer be in the oplog",
					// "CappedPositionLost",  // решили пока ретраить
				}
				for _, errSubstring := range fatalErrorSubstrings {
					if strings.Contains(err.Error(), errSubstring) {
						err = abstract.NewFatalError(err)
					}
				}

				// increase metric on fatal error
				if abstract.IsFatal(err) {
					s.metrics.Fatal.Inc()
				}

				// try to recover
				currentWatcher, err = s.watcherFallback(currentWatcher, parallelizationUnit, err)
				// if error persist, call shutdown
				if err != nil {
					s.shutdown(err)
					if currentWatcher != nil {
						currentWatcher.Close(s.ctx)
					}
					return
				}
			}
			s.logger.Error("Unexpected nil currentWatcher")
		}(parallelizationUnit, watcher, eventPusher)
	}
	s.writeWg.Add(1)
	go s.writeChanges()

	s.readTimer.Reset(readTimeout)
	pingTimeout := time.NewTimer(0) // starts immediately
	for {
		select {
		// time refreshes when change item channel receives new items
		case <-s.readTimer.C:
			s.shutdown(xerrors.Errorf("source read timeout reached"))
		case err := <-s.doneChannel:
			return err
		case <-pingTimeout.C:
			if err := s.pingParallelizationUnits(changeStreams); err != nil {
				s.logger.Warn("Unable to ping parallelization units", log.Error(err))
			}
			pingTimeout.Reset(time.Second * 10)
		}
	}
}

func (s *mongoSource) pingParallelizationUnits(changeStreams map[ParallelizationUnit]ChangeStreamWatcher) error {
	var errSample error
	var puList []string
	for pu := range changeStreams {
		if err := pu.Ping(s.ctx, s.client); err != nil {
			errSample = err
			puList = append(puList, pu.String())
		}
	}
	if errSample != nil {
		return xerrors.Errorf("sample error: %w; errors occured in this parallelization units: %v", errSample, puList)
	}
	return nil
}

// used for extra processing error
// it is possible to implement here fallback mechanics on different behaviour (ChangeStreamWatcher) when certain errors occur
func (s *mongoSource) watcherFallback(watcher ChangeStreamWatcher, pu ParallelizationUnit, reason error) (ChangeStreamWatcher, error) {
	// always close watcher, it will not be reused on fallback
	defer func() {
		if watcher != nil {
			watcher.Close(s.ctx)
		}
	}()
	if isFatalMongoCode(reason) {
		return nil, abstract.NewFatalError(reason)
	}

	if puDB, isPUDB := pu.(ParallelizationUnitDatabase); isPUDB && (strings.Contains(reason.Error(), "BSONObjectTooLarge") || strings.Contains(reason.Error(), "Tried to create string longer than 16MB")) {
		// if it is BSON, we could implement fallback mechanics here
		// TM-2234 -- decided it is not transparent to user, no real fallbacks will be performed
		// report undesired fullCollectionName and fail with fatal error
		var failingObjectName string
		if fullCollectionName, err := s.getFailingNamespace(watcher, puDB.UnitDatabase); err != nil {
			s.logger.Warn("failed to obtain the namespace from a failing watcher", log.Error(err))
			failingObjectName = puDB.String()
		} else {
			failingObjectName = fmt.Sprintf("%s, collection %q", puDB.String(), fullCollectionName.GetFullName())
		}
		return nil, abstract.NewFatalError(xerrors.Errorf("too large object detected in %s: %w", failingObjectName, reason))
	}

	// default: no watcher and original error
	return nil, reason
}

func lastTSforJSON(lastTS map[ParallelizationUnit]primitive.Timestamp) map[string]primitive.Timestamp {
	res := map[string]primitive.Timestamp{}
	for k, v := range lastTS {
		res[k.String()] = v
	}
	return res
}

func (s *mongoSource) writeChanges() {
	defer s.writeWg.Done()

	inflight := make([]abstract.ChangeItem, 0)
	readyToFlush := true
	flushTimer := time.NewTimer(0)
	<-flushTimer.C

	lastAdvanceProgressTime := time.Now()
	lastTS := map[ParallelizationUnit]primitive.Timestamp{}
	advanceOplogProgress := func() {
		if time.Since(lastAdvanceProgressTime) < 15*time.Second || len(lastTS) == 0 {
			return
		}
		var lastEventTimestamp *primitive.Timestamp
		for pu, v := range lastTS {
			if lastEventTimestamp == nil {
				lastEventTimestamp = &v
			}
			if lastEventTimestamp.T > v.T {
				lastEventTimestamp = &v
			}

			if err := pu.SaveClusterTime(s.ctx, s.client, &v); err != nil {
				s.logger.Warn("Cannot save cluster time",
					log.String("parallelization_unit", pu.String()), log.Error(err))
				return
			}
			s.logger.Info(fmt.Sprintf("update cluster time to: %v", lastEventTimestamp),
				log.Any("last_ts", lastTSforJSON(lastTS)),
				log.String("parallelization_unit", pu.String()),
			)
		}
		lastAdvanceProgressTime = time.Now()
		lastTS = map[ParallelizationUnit]primitive.Timestamp{}
	}

	flush := func() error {
		pushStart := time.Now()
		if err := <-s.sink.AsyncPush(inflight); err != nil {
			return xerrors.Errorf("Cannot push changes: %w", err)
		}
		s.metrics.PushTime.RecordDuration(time.Since(pushStart))

		inflight = inflight[:0]
		readyToFlush = false
		flushTimer.Reset(1 * time.Second)
		advanceOplogProgress()
		return nil
	}

	for {
		select {
		case chgEvent, ok := <-s.changeEvents:
			if !ok {
				return
			}
			// reset timer when change keyEvent appears
			s.readTimer.Reset(readTimeout)
			lastTS[chgEvent.parallelizationUnit] = chgEvent.event.ClusterTime
			s.metrics.Size.Add(int64(chgEvent.size))
			s.metrics.Count.Inc()
			chgItem, err := s.makeChangeItem(chgEvent)
			if !s.config.IsHomo {
				err := strictify.Strictify(&chgItem, DocumentSchema.Columns.FastColumns())
				if err != nil {
					go s.shutdown(abstract.NewFatalError(
						xerrors.Errorf("non strict value in hetero transfer: %w", err),
					))
					return
				}
			}
			if err != nil {
				if strings.Contains(err.Error(), "Empty FullDocument") {
					// silence this annoying error
				} else {
					s.logger.Warn("Cannot process change keyEvent", log.Error(err), log.Any("chgEvent", &chgEvent.event))
				}
				s.metrics.Unparsed.Inc()
				continue
			}
			if chgItem.Kind == abstract.MongoNoop {
				// noop is for refreshing ClusterTime
				advanceOplogProgress()
				continue
			}
			s.metrics.ChangeItems.Inc()
			if chgItem.IsRowEvent() {
				s.metrics.Parsed.Inc()
			}
			s.metrics.DelayTime.RecordDuration(time.Since(time.Unix(int64(chgEvent.event.ClusterTime.T), 0)))
			s.metrics.DecodeTime.RecordDuration(chgEvent.decodeTime)
			inflight = append(inflight, chgItem)
			if readyToFlush {
				if err := flush(); err != nil {
					go s.shutdown(err)
					return
				}
			}
		case <-flushTimer.C:
			if len(inflight) > 0 {
				if err := flush(); err != nil {
					go s.shutdown(err)
					return
				}
			} else {
				readyToFlush = true
			}
		}
	}
}

func getClusterTimeInNanosec(chgEvent *ChangeEvent) uint64 {
	return uint64(chgEvent.ClusterTime.T) * 1000000000
}

func (s *mongoSource) getFailingNamespace(watcher ChangeStreamWatcher, dbName string) (*Namespace, error) {
	nsRetriever, err := NewOneshotNamespaceRetriever(s, watcher.GetResumeToken(), dbName)
	defer func() {
		if nsRetriever != nil {
			nsRetriever.Close(s.ctx)
		}
	}()
	if err != nil {
		return nil, xerrors.Errorf("failed to create namespace-only receiver: %w", err)
	}
	collection, err := nsRetriever.GetNamespace(s.ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get namespace from namespace-only receiver: %w", err)
	}
	return collection, nil
}

func (s *mongoSource) makeChangeItem(readEvent *puChangeEvent) (abstract.ChangeItem, error) {
	chgEvent := readEvent.event
	rawEventSize := uint64(readEvent.size)
	switch chgEvent.OperationType {
	case "insert":
		item := DExt(chgEvent.FullDocument)
		if item == nil {
			return abstract.ChangeItem{}, xerrors.New("Empty FullDocument")
		}
		val := item.Value(s.config.IsHomo, s.config.PreventJSONRepack)
		key, err := s.extractKey(chgEvent.DocumentKey.ID)
		if err != nil {
			return abstract.ChangeItem{}, xerrors.Errorf("cannot extract key for insert event: %w", err)
		}
		return abstract.ChangeItem{
			ID:           0,
			LSN:          uint64(chgEvent.ClusterTime.T),
			CommitTime:   getClusterTimeInNanosec(chgEvent),
			Counter:      int(chgEvent.ClusterTime.I),
			Kind:         abstract.InsertKind,
			Schema:       chgEvent.Namespace.Database,
			Table:        chgEvent.Namespace.Collection,
			PartID:       "",
			ColumnNames:  DocumentSchema.ColumnsNames,
			ColumnValues: []interface{}{key, val},
			TableSchema:  DocumentSchema.Columns,
			OldKeys:      *new(abstract.OldKeysType),
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(rawEventSize),
		}, nil
	case "delete":
		key, err := s.extractKey(chgEvent.DocumentKey.ID)
		if err != nil {
			return abstract.ChangeItem{}, xerrors.Errorf("cannot extract key for delete event: %w", err)
		}
		return abstract.ChangeItem{
			ID:           0,
			LSN:          uint64(chgEvent.ClusterTime.T),
			CommitTime:   getClusterTimeInNanosec(chgEvent),
			Counter:      int(chgEvent.ClusterTime.I),
			Kind:         abstract.DeleteKind,
			Schema:       chgEvent.Namespace.Database,
			Table:        chgEvent.Namespace.Collection,
			PartID:       "",
			ColumnNames:  nil,
			ColumnValues: nil,
			TableSchema:  DocumentSchema.Columns,
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"_id"},
				KeyTypes:  nil,
				KeyValues: []interface{}{key},
			},
			TxID:  "",
			Query: "",
			Size:  abstract.RawEventSize(rawEventSize),
		}, nil
	case "update", "replace":
		key, err := s.extractKey(chgEvent.DocumentKey.ID)
		if err != nil {
			return abstract.ChangeItem{}, xerrors.Errorf("cannot extract key for update event: %w", err)
		}
		if chgEvent.OperationType == "update" && chgEvent.UpdateDescription != nil && s.config.IsHomo {
			return abstract.ChangeItem{
				ID:          0,
				LSN:         uint64(chgEvent.ClusterTime.T),
				CommitTime:  getClusterTimeInNanosec(chgEvent),
				Counter:     int(chgEvent.ClusterTime.I),
				Kind:        abstract.MongoUpdateDocumentKind,
				Schema:      chgEvent.Namespace.Database,
				Table:       chgEvent.Namespace.Collection,
				PartID:      "",
				ColumnNames: UpdateDocumentSchema.ColumnsNames,
				ColumnValues: []interface{}{
					chgEvent.DocumentKey.ID,
					chgEvent.UpdateDescription.UpdatedFields,
					chgEvent.UpdateDescription.RemovedFields,
					chgEvent.UpdateDescription.TruncatedArrays,
					DExt(chgEvent.FullDocument).RawValue(),
				},
				TableSchema: UpdateDocumentSchema.Columns,
				OldKeys: abstract.OldKeysType{
					KeyNames:  []string{"_id"},
					KeyTypes:  nil,
					KeyValues: []interface{}{key},
				},
				TxID:  "",
				Query: "",
				Size:  abstract.RawEventSize(rawEventSize),
			}, nil
		}
		item := DExt(chgEvent.FullDocument)
		if item == nil {
			return abstract.ChangeItem{}, xerrors.New("Empty FullDocument")
		}
		val := item.Value(s.config.IsHomo, s.config.PreventJSONRepack)
		return abstract.ChangeItem{
			ID:           0,
			LSN:          uint64(chgEvent.ClusterTime.T),
			CommitTime:   getClusterTimeInNanosec(chgEvent),
			Counter:      int(chgEvent.ClusterTime.I),
			Kind:         abstract.UpdateKind,
			Schema:       chgEvent.Namespace.Database,
			Table:        chgEvent.Namespace.Collection,
			PartID:       "",
			ColumnNames:  DocumentSchema.ColumnsNames,
			ColumnValues: []interface{}{key, val},
			TableSchema:  DocumentSchema.Columns,
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"_id"},
				KeyTypes:  nil,
				KeyValues: []interface{}{key},
			},
			TxID:  "",
			Query: "",
			Size:  abstract.RawEventSize(rawEventSize),
		}, nil
	case "create":
		return abstract.ChangeItem{
			ID:           0,
			LSN:          uint64(chgEvent.ClusterTime.T),
			CommitTime:   getClusterTimeInNanosec(chgEvent),
			Counter:      int(chgEvent.ClusterTime.I),
			Kind:         abstract.MongoCreateKind,
			Schema:       chgEvent.Namespace.Database,
			Table:        chgEvent.Namespace.Collection,
			PartID:       "",
			ColumnNames:  nil,
			ColumnValues: nil,
			TableSchema:  DocumentSchema.Columns,
			OldKeys:      *new(abstract.OldKeysType),
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(rawEventSize),
		}, nil
	case "drop":
		return abstract.ChangeItem{
			ID:           0,
			LSN:          uint64(chgEvent.ClusterTime.T),
			CommitTime:   getClusterTimeInNanosec(chgEvent),
			Counter:      int(chgEvent.ClusterTime.I),
			Kind:         abstract.MongoDropKind,
			Schema:       chgEvent.Namespace.Database,
			Table:        chgEvent.Namespace.Collection,
			PartID:       "",
			ColumnNames:  nil,
			ColumnValues: nil,
			TableSchema:  DocumentSchema.Columns,
			OldKeys:      *new(abstract.OldKeysType),
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(rawEventSize),
		}, nil
	case "rename":
		return abstract.ChangeItem{
			ID:          0,
			LSN:         uint64(chgEvent.ClusterTime.T),
			CommitTime:  getClusterTimeInNanosec(chgEvent),
			Counter:     int(chgEvent.ClusterTime.I),
			Kind:        abstract.MongoRenameKind,
			Schema:      chgEvent.Namespace.Database,
			Table:       chgEvent.Namespace.Collection,
			PartID:      "",
			ColumnNames: []string{"namespace"},
			ColumnValues: []interface{}{
				chgEvent.ToNamespace,
			},
			TableSchema: DocumentSchema.Columns,
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"namespace"},
				KeyTypes:  nil,
				KeyValues: []interface{}{chgEvent.Namespace},
			},
			TxID:  "",
			Query: "",
			Size:  abstract.RawEventSize(rawEventSize),
		}, nil
	case "dropDatabase":
		return abstract.ChangeItem{
			ID:           0,
			LSN:          uint64(chgEvent.ClusterTime.T),
			CommitTime:   getClusterTimeInNanosec(chgEvent),
			Counter:      int(chgEvent.ClusterTime.I),
			Kind:         abstract.MongoDropDatabaseKind,
			Schema:       chgEvent.Namespace.Database,
			Table:        chgEvent.Namespace.Collection,
			PartID:       "",
			ColumnNames:  nil,
			ColumnValues: nil,
			TableSchema:  DocumentSchema.Columns,
			OldKeys:      *new(abstract.OldKeysType),
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(rawEventSize),
		}, nil
	case "noop":
		return abstract.ChangeItem{
			ID:           0,
			LSN:          uint64(chgEvent.ClusterTime.T),
			CommitTime:   getClusterTimeInNanosec(chgEvent),
			Counter:      int(chgEvent.ClusterTime.I),
			Kind:         abstract.MongoNoop,
			Schema:       "",
			Table:        "",
			PartID:       "",
			ColumnNames:  nil,
			ColumnValues: nil,
			TableSchema:  DocumentSchema.Columns,
			OldKeys:      *new(abstract.OldKeysType),
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(rawEventSize),
		}, nil
	default:
		return abstract.ChangeItem{}, xerrors.New(fmt.Sprintf("unsupported operation type: %v", chgEvent.OperationType))
	}
}

func (s *mongoSource) getCollectionFilter(client *MongoClientWrapper) (*MongoCollectionFilter, error) {
	var result MongoCollectionFilter
	if len(s.filter.Collections) == 0 {
		allDatabaseCollections, err := GetAllExistingCollections(s.ctx, client)
		if err != nil {
			return nil, xerrors.Errorf("couldn't get all collection list: %w", err)
		}
		result.Collections = allDatabaseCollections
		result.ExcludedCollections = s.config.ExcludedCollections
		s.logger.Info("Update filter", log.Any("filter_original", s.config.GetMongoCollectionFilter()), log.Any("filter_actual", s.filter))
	} else {
		result = s.filter
	}
	return &result, nil
}

// openChangeStreams is mongo version dependent function
func (s *mongoSource) openChangeStreams() (map[ParallelizationUnit]ChangeStreamWatcher, error) {
	mongoVersion, err := GetVersion(s.ctx, s.client, s.config.AuthSource)
	if err != nil {
		return nil, xerrors.Errorf("cannot get version: %w", err)
	}

	replicationSource := ReplicationSourceFallback(s.logger, s.config.ReplicationSource, mongoVersion)
	s.logger.Info("Opening change streams according to configuration",
		log.Any("mongo_version", mongoVersion),
		log.Any("replication_source", s.config.ReplicationSource),
		log.Any("fallback_replication_source", replicationSource),
	)

	switch replicationSource {
	case MongoReplicationSourceUnspecified:
		fallthrough
	case MongoReplicationSourcePerDatabase, MongoReplicationSourcePerDatabaseFullDocument, MongoReplicationSourcePerDatabaseUpdateDocument:
		if changeStreams, err := s.openPerDatabaseChangeStreams(replicationSource); err != nil {
			return nil, xerrors.Errorf("couldn't open per-database change streams: %w", err)
		} else {
			return changeStreams, nil
		}
	case MongoReplicationSourceOplog:
		if changeStreams, err := s.openOplogChangeStream(); err != nil {
			return nil, xerrors.Errorf("couldn't open oplog change stream: %w", err)
		} else {
			return changeStreams, nil
		}
	default:
		return nil, xerrors.Errorf("Unknown parallelization level: %s", string(replicationSource))
	}
}

func (s *mongoSource) openPerDatabaseChangeStreams(rs MongoReplicationSource) (map[ParallelizationUnit]ChangeStreamWatcher, error) {
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	changeStreams := map[ParallelizationUnit]ChangeStreamWatcher{}

	changeStreamsProcessed := map[string]struct{}{}
	updatedCollectionFilter, err := s.getCollectionFilter(s.client)
	if err != nil {
		return nil, xerrors.Errorf("cannot get collection filter: %w", err)
	}
	s.filter = *updatedCollectionFilter

	s.logger.Info("Infer collections", log.Any("collection", s.filter.Collections))
	dbs := map[string]bool{}
	for _, col := range s.filter.Collections {
		dbs[col.DatabaseName] = true
	}

	s.logger.Info("Initialize change stream watchers",
		log.String("replication_source", string(rs)))
	for dbName := range dbs {
		if _, ok := changeStreamsProcessed[dbName]; ok {
			continue
		}

		pu := MakeParallelizationUnitDatabase(s.config.TechnicalDatabase, s.config.SlotID, dbName)
		// choose watcher depending on input parameter
		var watcher ChangeStreamWatcher
		var err error
		switch rs {
		case MongoReplicationSourceUnspecified:
			fallthrough
		case MongoReplicationSourcePerDatabase:
			watcher, err = NewDatabaseDocumentKeyWatcher(s, pu)
		case MongoReplicationSourcePerDatabaseFullDocument, MongoReplicationSourcePerDatabaseUpdateDocument:
			watcher, err = NewDatabaseFullDocumentWatcher(s, pu, rs)
		default:
			return nil, abstract.NewFatalError(xerrors.Errorf("Inapplicable replication source: %s", rs))
		}

		if xerrors.Is(err, ErrEmptyFilter) {
			// skip database with filter passing no collection
			s.logger.Warn("Filter for database is empty! Check the rules:",
				log.String("database", dbName),
				log.Any("included", s.config.Collections),
				log.Any("excluded", s.config.ExcludedCollections),
				log.Error(err))
			continue
		}
		if err != nil {
			s.logger.Error("Cannot open change stream for database",
				log.String("database", dbName),
				log.Error(err))

			return nil, xerrors.Errorf("failed to open change stream for database %q: %w", dbName, err)
		}
		rollbacks.Add(func() { watcher.Close(s.ctx) })
		changeStreams[pu] = watcher
		changeStreamsProcessed[dbName] = struct{}{}
	}

	rollbacks.Cancel() // all watchers initialized, cancel their closure
	return changeStreams, nil
}

func (s *mongoSource) openOplogChangeStream() (map[ParallelizationUnit]ChangeStreamWatcher, error) {
	oplogPu := MakeParallelizationUnitOplog(s.config.TechnicalDatabase, s.config.SlotID)
	clusterTime, err := oplogPu.GetClusterTime(s.ctx, s.client)
	if err != nil {
		return nil, xerrors.Errorf("error getting oplog cluster time: %w", err)
	}
	cs, err := newOplogWatcher(s.logger, s.client, *clusterTime, s.filterOplogWithRegex, s.config)
	if err != nil {
		return nil, xerrors.Errorf("error making oplog watcher: %w", err)
	}
	return map[ParallelizationUnit]ChangeStreamWatcher{oplogPu: cs}, nil
}

func (s *mongoSource) Stop() {
	s.shutdown(nil)
}

func (s *mongoSource) shutdown(shutdownErr error) {
	s.logger.Warn("Shutdown source", log.Error(shutdownErr))
	s.onceShutdown.Do(func() {
		s.cancel()
		if s.changeEvents == nil { // Not started yet
			close(s.doneChannel)
			return
		}

		// Stop asynchronously
		go func() {
			s.readWg.Wait()
			close(s.changeEvents)
			s.writeWg.Wait()
			s.logger.Info("MongoDB replication source stopped")
			if shutdownErr != nil {
				s.doneChannel <- shutdownErr
			} else {
				close(s.doneChannel)
			}
		}()
	})
}

func (s *mongoSource) makeDatabaseConnection(dbName string) (*mongo.Database, error) {
	dbOpts := options.Database()
	if s.config.SecondaryPreferredMode {
		readPref, err := readpref.New(readpref.SecondaryPreferredMode)
		if err != nil {
			return nil, abstract.NewFatalError(xerrors.Errorf("Cannot initialize read pref with SecondaryPreferredMode"))
		}
		dbOpts.SetReadPreference(readPref)
	}
	db := s.client.Database(dbName, dbOpts)
	return db, nil
}

func (s *mongoSource) extractKey(id interface{}) (interface{}, error) {
	return ExtractKey(id, s.config.IsHomo)
}

func overrideFilter(filter MongoCollectionFilter, includeTables map[abstract.TableID]bool) MongoCollectionFilter {
	if len(includeTables) > 0 {
		filter.Collections = make([]MongoCollection, 0)
		for tid := range includeTables {
			filter.Collections = append(filter.Collections, MongoCollection{
				DatabaseName:   tid.Namespace,
				CollectionName: tid.Name,
			})
		}
	}
	return filter
}

func NewSource(src *MongoSource, transferID string, objects *model.DataObjects, logger log.Logger, registry metrics.Registry, cp coordinator.Coordinator) (abstract.Source, error) {
	includeObjects, err := abstract.BuildIncludeMap(objects.GetIncludeObjects())
	if err != nil {
		return nil, abstract.NewFatalError(xerrors.Errorf("to build exclude map: %w", err))
	}
	ctx, cancel := context.WithCancel(context.Background())
	doneChannel := make(chan error)
	return &mongoSource{
		logger:               logger,
		metrics:              stats.NewSourceStats(registry),
		cp:                   cp,
		config:               src,
		filter:               overrideFilter(src.GetMongoCollectionFilter(), includeObjects),
		ctx:                  ctx,
		cancel:               cancel,
		doneChannel:          doneChannel,
		onceShutdown:         sync.Once{},
		sink:                 nil,
		client:               nil,
		readWg:               sync.WaitGroup{},
		writeWg:              sync.WaitGroup{},
		changeEvents:         nil,
		readTimer:            time.NewTimer(readTimeout),
		transferID:           transferID,
		filterOplogWithRegex: src.FilterOplogWithRegexp,
	}, nil
}
