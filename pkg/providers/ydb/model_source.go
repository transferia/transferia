//go:build !disable_ydb_provider

package ydb

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	v3credential "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type YdbColumnsFilterType string

const (
	YdbColumnsBlackList YdbColumnsFilterType = "blacklist"
	YdbColumnsWhiteList YdbColumnsFilterType = "whitelist"
)

type ChangeFeedModeType string

const (
	ChangeFeedModeUpdates         ChangeFeedModeType = "UPDATES"
	ChangeFeedModeNewImage        ChangeFeedModeType = "NEW_IMAGE"
	ChangeFeedModeNewAndOldImages ChangeFeedModeType = "NEW_AND_OLD_IMAGES"
)

type CommitMode string

const (
	CommitModeUnspecified CommitMode = ""
	CommitModeAsync       CommitMode = "ASYNC"
	CommitModeNone        CommitMode = "NONE"
	CommitModeSync        CommitMode = "SYNC"
)

type YdbColumnsFilter struct {
	TableNamesRegexp  string
	ColumnNamesRegexp string
	Type              YdbColumnsFilterType
}

type YdbSource struct {
	Database           string
	Instance           string
	Tables             []string // actually it's 'paths', but migrating...
	TableColumnsFilter []YdbColumnsFilter
	SubNetworkID       string
	SecurityGroupIDs   []string
	Underlay           bool
	UseFullPaths       bool // can be useful to deal with names collision

	TLSEnabled  bool
	RootCAFiles []string

	// replication stuff:
	ChangeFeedMode               ChangeFeedModeType
	ChangeFeedRetentionPeriod    *time.Duration // not suitable for pre-created (custom) changefeed
	ChangeFeedCustomName         string         // user can specify pre-created feed's name, otherwise it will created with name == transferID
	ChangeFeedCustomConsumerName string
	BufferSize                   model.BytesSize // it's not some real buffer size - see comments to waitLimits() method in kafka-source
	VerboseSDKLogs               bool
	CommitMode                   CommitMode

	// auth stuff:
	Token            model.SecretString
	UserdataAuth     bool
	ServiceAccountID string
	TokenServiceURL  string
	SAKeyContent     string
	OAuth2Config     *v3credential.OAuth2Config

	// storage
	IsSnapshotSharded     bool
	CopyFolder            string
	ParseQueueParallelism int
}

var _ model.Source = (*YdbSource)(nil)

func (s *YdbSource) MDBClusterID() string {
	return s.Instance + s.Database
}

func (s *YdbSource) IsSource() {}

func (s *YdbSource) WithDefaults() {
	if s.ChangeFeedMode == "" {
		s.ChangeFeedMode = ChangeFeedModeNewImage
	}
	if s.BufferSize == 0 {
		s.BufferSize = 100 * 1024 * 1024
	}
}

func (s *YdbSource) Include(tID abstract.TableID) bool {
	return len(s.FulfilledIncludes(tID)) > 0
}

func makePaths(currPath string) (string, string) {
	currPathWithTrailingSlash := currPath
	currPathWithoutTrailingSlash := currPath
	if strings.HasSuffix(currPath, "/") {
		currPathWithoutTrailingSlash = strings.TrimSuffix(currPath, "/")
	} else {
		currPathWithTrailingSlash = currPath + "/"
	}
	return currPathWithTrailingSlash, currPathWithoutTrailingSlash
}

func MakeYDBRelPath(useFullPaths bool, paths []string, tableName string) string {
	tableName = strings.TrimLeft(tableName, "/")
	if !useFullPaths {
		for _, folderPath := range paths {
			folderPath = strings.TrimLeft(folderPath, "/")
			folderPathWithTrailingSlash, folderPathWithoutTrailingSlash := makePathsTrailingSlashVariants(folderPath)
			if tableName == folderPath || strings.HasPrefix(tableName, folderPathWithTrailingSlash) {
				basePath := filepath.Dir(folderPathWithoutTrailingSlash)
				result := strings.TrimPrefix(tableName, basePath+"/")
				return strings.TrimPrefix(result, "/")
			}
		}
	}
	return tableName
}

func MatchchangeFeedMode(ydbMode options.ChangefeedMode) ChangeFeedModeType {
	switch ydbMode {
	case options.ChangefeedModeUpdates:
		return ChangeFeedModeUpdates
	case options.ChangefeedModeNewAndOldImages:
		return ChangeFeedModeNewAndOldImages
	case options.ChangefeedModeNewImage:
		return ChangeFeedModeNewImage
	default:
		return ""
	}
}

func ConvertTableMapToYDBRelPath(params *YdbStorageParams, tableMap abstract.TableMap) abstract.TableMap {
	result := abstract.TableMap{}
	for tableID, tableInfo := range tableMap {
		newTableID := tableID
		newTableID.Name = MakeYDBRelPath(params.UseFullPaths, params.Tables, tableID.Name)
		result[newTableID] = tableInfo
	}
	return result
}

func (s *YdbSource) FulfilledIncludes(tableID abstract.TableID) []string {
	if len(s.Tables) == 0 { // 'root' case
		return []string{""}
	} else { // 'tables & directories' case
		for _, originalPath := range s.Tables {
			path := strings.TrimLeft(originalPath, "/")
			tableName := strings.TrimLeft(tableID.Name, "/")
			pathWithTrailingSlash, _ := makePaths(path)
			if tableName == path || strings.HasPrefix(tableName, pathWithTrailingSlash) {
				return []string{originalPath}
			}
		}
		return nil
	}
}

func (s *YdbSource) AllIncludes() []string {
	return s.Tables
}

func (s *YdbSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *YdbSource) Validate() error {
	return nil
}

func (s *YdbSource) ExtraTransformers(_ context.Context, _ *model.Transfer, _ metrics.Registry) ([]abstract.Transformer, error) {
	var result []abstract.Transformer
	if !s.UseFullPaths {
		result = append(result, NewYDBRelativePathTransformer(s.Tables))
	}
	return result, nil
}

func (s *YdbSource) ToStorageParams() *YdbStorageParams {
	return &YdbStorageParams{
		Database:           s.Database,
		Instance:           s.Instance,
		Tables:             s.Tables,
		TableColumnsFilter: s.TableColumnsFilter,
		UseFullPaths:       s.UseFullPaths,
		Token:              s.Token,
		ServiceAccountID:   s.ServiceAccountID,
		UserdataAuth:       s.UserdataAuth,
		SAKeyContent:       s.SAKeyContent,
		TokenServiceURL:    s.TokenServiceURL,
		OAuth2Config:       s.OAuth2Config,
		RootCAFiles:        s.RootCAFiles,
		TLSEnabled:         s.TLSEnabled,
		IsSnapshotSharded:  s.IsSnapshotSharded,
		CopyFolder:         s.CopyFolder,
	}
}

func (*YdbSource) IsIncremental()                 {}
func (*YdbSource) SupportsStartCursorValue() bool { return true }
