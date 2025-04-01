package ydb

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	v3credential "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

type YdbDestination struct {
	Token                   model.SecretString
	Database                string
	Path                    string
	Instance                string
	LegacyWriter            bool
	ShardCount              int64
	Rotation                *model.RotatorConfig
	TransformerConfig       map[string]string
	AltNames                map[string]string
	StoragePolicy           string
	CompactionPolicy        string
	SubNetworkID            string
	SecurityGroupIDs        []string
	Cleanup                 model.CleanupType
	DropUnknownColumns      bool
	Underlay                bool
	ServiceAccountID        string
	IgnoreRowTooLargeErrors bool
	FitDatetime             bool // will crop date-time to allowed time range (with data-loss)
	SAKeyContent            string
	TriggingInterval        time.Duration
	TriggingSize            uint64
	IsTableColumnOriented   bool
	DefaultCompression      string

	Primary bool // if worker is first, i.e. primary, will run background jobs

	TLSEnabled      bool
	RootCAFiles     []string
	TokenServiceURL string
	UserdataAuth    bool // allow fallback to Instance metadata Auth
	OAuth2Config    *v3credential.OAuth2Config
}

var _ model.Destination = (*YdbDestination)(nil)

func (d *YdbDestination) MDBClusterID() string {
	return d.Instance + d.Database
}

func (YdbDestination) IsDestination() {
}

func (d *YdbDestination) WithDefaults() {
	if d.Cleanup == "" {
		d.Cleanup = model.Drop
	}
	if d.DefaultCompression == "" {
		d.DefaultCompression = "off"
	}
}

func (d *YdbDestination) CleanupMode() model.CleanupType {
	return d.Cleanup
}

func (d *YdbDestination) Transformer() map[string]string {
	return d.TransformerConfig
}

func (d *YdbDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *YdbDestination) Validate() error {
	d.Rotation = d.Rotation.NilWorkaround()
	if err := d.Rotation.Validate(); err != nil {
		return err
	}
	return nil
}

func (d *YdbDestination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    0,
		TriggingSize:     d.TriggingSize,
		TriggingInterval: d.TriggingInterval,
	}
}

func (d *YdbDestination) ToStorageParams() *YdbStorageParams {
	return &YdbStorageParams{
		Database:           d.Database,
		Instance:           d.Instance,
		Tables:             nil,
		TableColumnsFilter: nil,
		UseFullPaths:       false,
		Token:              d.Token,
		ServiceAccountID:   d.ServiceAccountID,
		UserdataAuth:       d.UserdataAuth,
		SAKeyContent:       d.SAKeyContent,
		TokenServiceURL:    d.TokenServiceURL,
		OAuth2Config:       d.OAuth2Config,
		RootCAFiles:        d.RootCAFiles,
		TLSEnabled:         false,
		IsSnapshotSharded:  false,
		CopyFolder:         "",
	}
}
