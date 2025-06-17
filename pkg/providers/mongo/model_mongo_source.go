//go:build !disable_mongo_provider

package mongo

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// TODO(@kry127) tune constant, or make more deterministic approach of measuring pipeline BSON size
	DefaultKeySizeThreshold   = 4 * 1024 * 1024 // soft upper border for batch size in bytes (if one key is bigger, it'll fit)
	DefaultBatchFlushInterval = 5 * time.Second // batch is guaranteed to be flushed every five seconds, but it may flush more frequently
	DefaultBatchSizeLimit     = 500             // limit of amount of events inside one batch

	// desired part size of collection
	TablePartByteSize = 1024 * 1024 * 1024

	DataTransferSystemDatabase = "__data_transfer"
)

type MongoSource struct {
	ClusterID              string
	Hosts                  []string
	Port                   int
	ReplicaSet             string
	AuthSource             string
	User                   string
	Password               model.SecretString
	Collections            []MongoCollection
	ExcludedCollections    []MongoCollection
	SubNetworkID           string
	SecurityGroupIDs       []string
	TechnicalDatabase      string // deprecated: should be always ""
	IsHomo                 bool
	SlotID                 string // It's synthetic entity. Always equal to transfer_id!
	SecondaryPreferredMode bool
	TLSFile                string
	ReplicationSource      MongoReplicationSource
	BatchingParams         *BatcherParameters // for now this is private params
	DesiredPartSize        uint64
	PreventJSONRepack      bool // should not be used, use cases for migration: DTSUPPORT-1596
	ConnectionID           string

	// FilterOplogWithRegexp is matters when ReplicationSource==MongoReplicationSourceOplog
	//
	// When it is set to false (recommended default value): no filtering of oplog will happen
	//
	// When it is set to true, oplog events are filtered with regexp build according to
	// Collections and ExcludedCollections
	// Regexp unconditionally includes collections '$cmd.*' for technical events and '' for noops
	// + Advantage of this mode: network efficiency
	// - Disadvantage of this mode: when there are no changes on listened database, oplog will be lost
	//   TODO(@kry127) Consider turning on consumer keeper in this case (separate tech db + filtering will be enough)
	FilterOplogWithRegexp bool
	// make a `direct` connection to mongo, see: https://www.mongodb.com/docs/drivers/go/current/fundamentals/connections/connection-guide/
	Direct bool

	RootCAFiles []string
	// indicates whether the mongoDB client uses a mongodb+srv connection
	SRVMode bool
}

var (
	_ model.Source           = (*MongoSource)(nil)
	_ model.WithConnectionID = (*MongoSource)(nil)
)

type BatcherParameters struct {
	BatchSizeLimit     uint
	KeySizeThreshold   uint64
	BatchFlushInterval time.Duration
}

type MongoCollectionFilter struct {
	Collections         []MongoCollection
	ExcludedCollections []MongoCollection
}

type MongoCollection struct {
	DatabaseName   string
	CollectionName string
}

func (c MongoCollection) String() string {
	return fmt.Sprintf("%v.%v", c.DatabaseName, c.CollectionName)
}

func ToRegexp(c MongoCollection) string {
	switch {
	case c.DatabaseName == "*" && c.CollectionName == "*":
		return ".*\\.*"
	case c.CollectionName == "*":
		return fmt.Sprintf("%s\\.%s", regexp.QuoteMeta(c.DatabaseName), ".*")
	case c.DatabaseName == "*":
		return fmt.Sprintf("%s\\.%s", ".*", regexp.QuoteMeta(c.CollectionName))
	default:
		return regexp.QuoteMeta(c.String())
	}
}

func NewMongoCollection(planeName string) *MongoCollection {
	id := strings.Index(planeName, ".")
	if id == -1 {
		return nil
	}
	return &MongoCollection{
		DatabaseName:   planeName[:id],
		CollectionName: planeName[id+1:],
	}
}

func (s *MongoSource) MDBClusterID() string {
	return s.ClusterID
}

func (s *MongoSource) WithDefaults() {
	if s.Port == 0 {
		s.Port = 27018
	}

	if len(s.Hosts) > 1 {
		sort.Strings(s.Hosts)
	}
	if s.BatchingParams == nil {
		s.BatchingParams = &BatcherParameters{
			BatchSizeLimit:     DefaultBatchSizeLimit,
			KeySizeThreshold:   DefaultKeySizeThreshold,
			BatchFlushInterval: DefaultBatchFlushInterval,
		}
	}

	if s.DesiredPartSize == 0 {
		s.DesiredPartSize = TablePartByteSize
	}
}

func (s *MongoSource) fulfilledIncludesImpl(tID abstract.TableID, firstIncludeOnly bool) (result []string) {
	for _, coll := range s.ExcludedCollections {
		if coll.DatabaseName == tID.Namespace && coll.CollectionName == "*" {
			return result
		}
		if coll.DatabaseName == tID.Namespace && coll.CollectionName == tID.Name {
			return result
		}
	}
	if len(s.Collections) == 0 {
		return []string{""}
	}
	for _, coll := range s.Collections {
		if coll.DatabaseName == tID.Namespace && (coll.CollectionName == "*" || coll.CollectionName == tID.Name) {
			result = append(result, coll.String())
			if firstIncludeOnly {
				return result
			}
		}
	}
	return result
}

func (s *MongoSource) Include(tID abstract.TableID) bool {
	return len(s.fulfilledIncludesImpl(tID, true)) > 0
}

func (s *MongoSource) FulfilledIncludes(tID abstract.TableID) (result []string) {
	return s.fulfilledIncludesImpl(tID, false)
}

func (s *MongoSource) AllIncludes() []string {
	result := make([]string, 0)
	for _, coll := range s.Collections {
		result = append(result, coll.String())
	}
	return result
}

var ErrEmptyFilter = xerrors.New("Filters pass empty collection list")

// BuildPipeline returns mongo pipeline that should be able
// to filter the oplog from unwanted changes in database 'forDatabaseName'
func (f MongoCollectionFilter) BuildPipeline(forDatabaseName string) (mongo.Pipeline, error) {
	excluded := map[string]struct{}{}
	included := map[string]struct{}{}
	makeInExprFromMap := func(m map[string]struct{}) bson.D {
		list := bson.A{}
		for el := range m {
			list = append(list, el)
		}
		return bson.D{{Key: "$in", Value: list}}
	}
	for _, coll := range f.ExcludedCollections {
		if coll.DatabaseName == forDatabaseName {
			if coll.CollectionName == "*" {
				// should not match anything when all excluded
				return nil, ErrEmptyFilter
			}
			excluded[coll.CollectionName] = struct{}{}
		}
	}
	for _, coll := range f.Collections {
		if coll.DatabaseName == forDatabaseName {
			if coll.CollectionName == "*" {
				// TODO test $not
				return mongo.Pipeline{
					// just exclude excluded, others values are passing
					{{Key: "$match", Value: bson.D{{Key: "ns.coll", Value: bson.M{"$not": makeInExprFromMap(excluded)}}}}},
				}, nil
			}
			if _, ok := excluded[coll.CollectionName]; !ok {
				// if collection is not excluded, in can be included
				included[coll.CollectionName] = struct{}{}
			}
		}
	}
	if len(included) == 0 {
		// no included collections, return error of empty filter
		return nil, ErrEmptyFilter
	}
	// add consumer keeper for reading timestamp refreshes
	included[ClusterTimeCollName] = struct{}{}

	// mix included and excluded lists in a single pipeline
	matchFilters := bson.D{{Key: "ns.coll", Value: makeInExprFromMap(included)}}
	if len(excluded) > 0 {
		matchFilters = append(matchFilters, bson.E{Key: "ns.coll", Value: bson.M{"$not": makeInExprFromMap(excluded)}})
	}
	return mongo.Pipeline{{{Key: "$match", Value: matchFilters}}}, nil
}

func (s *MongoSource) GetMongoCollectionFilter() MongoCollectionFilter {
	return MongoCollectionFilter{
		Collections:         s.Collections,
		ExcludedCollections: s.ExcludedCollections,
	}
}

func (MongoSource) IsSource()       {}
func (MongoSource) IsStrictSource() {}

func (s *MongoSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *MongoSource) GetConnectionID() string {
	return s.ConnectionID
}

type MongoReplicationSource string

var (
	MongoReplicationSourceUnspecified               MongoReplicationSource = ""
	MongoReplicationSourcePerDatabase               MongoReplicationSource = "PerDatabase"
	MongoReplicationSourcePerDatabaseFullDocument   MongoReplicationSource = "PerDatabase_FullDocument"
	MongoReplicationSourcePerDatabaseUpdateDocument MongoReplicationSource = "PerDatabase_UpdateDocument"
	MongoReplicationSourceOplog                     MongoReplicationSource = "Oplog"
)

func (s *MongoSource) HasTLS() bool {
	return s.ClusterID != "" || s.TLSFile != ""
}

func (s *MongoSource) Validate() error {
	if s.IsHomo && s.PreventJSONRepack {
		return xerrors.Errorf("no sense in preventing JSON repacking when transfer is homogeneous")
	}
	return nil
}

func (s *MongoSource) ConnectionOptions(defaultCACertPaths []string) MongoConnectionOptions {
	return s.ToStorageParams().ConnectionOptions(defaultCACertPaths)
}

func (s *MongoSource) ToStorageParams() *MongoStorageParams {
	return &MongoStorageParams{
		TLSFile:           s.TLSFile,
		ClusterID:         s.ClusterID,
		Hosts:             s.Hosts,
		Port:              s.Port,
		ReplicaSet:        s.ReplicaSet,
		AuthSource:        s.AuthSource,
		User:              s.User,
		Password:          string(s.Password),
		Collections:       s.Collections,
		DesiredPartSize:   s.DesiredPartSize,
		PreventJSONRepack: s.PreventJSONRepack,
		Direct:            s.Direct,
		RootCAFiles:       s.RootCAFiles,
		SRVMode:           s.SRVMode,
		ConnectionID:      s.ConnectionID,
	}
}
