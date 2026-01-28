package ydb

import (
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/pkg/abstract/model"
	v3credential "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

const (
	defaultMaxStorageBatchLen  = 10000
	defaultMaxStorageBatchSize = 128 * humanize.MiByte
)

type YdbStorageParams struct {
	Database           string             `log:"true"`
	Instance           string             `log:"true"`
	Tables             []string           `log:"true"`
	TableColumnsFilter []YdbColumnsFilter `log:"true"`
	UseFullPaths       bool               `log:"true"`

	MaxBatchLen  int `log:"true"`
	MaxBatchSize int `log:"true"`

	// auth props
	Token            model.SecretString
	ServiceAccountID string `log:"true"`
	UserdataAuth     bool   `log:"true"`
	SAKeyContent     string
	TokenServiceURL  string `log:"true"`
	OAuth2Config     *v3credential.OAuth2Config

	RootCAFiles []string
	TLSEnabled  bool `log:"true"`

	IsSnapshotSharded bool   `log:"true"`
	CopyFolder        string `log:"true"`
}

func (p YdbStorageParams) MaxBatchLenOrDefault() int {
	if p.MaxBatchLen <= 0 {
		return defaultMaxStorageBatchLen
	}
	return p.MaxBatchLen
}

func (p YdbStorageParams) MaxBatchSizeOrDefault() int {
	if p.MaxBatchSize <= 0 {
		return defaultMaxStorageBatchSize
	}
	return p.MaxBatchSize
}
