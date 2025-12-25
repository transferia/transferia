package ydb

import (
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	v3credential "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"go.uber.org/zap/zapcore"
)

type YdbDestination struct {
	Token                     model.SecretString
	Database                  string               `log:"true"`
	Path                      string               `log:"true"`
	Instance                  string               `log:"true"`
	LegacyWriter              bool                 `log:"true"`
	ShardCount                int64                `log:"true"`
	Rotation                  *model.RotatorConfig `log:"true"`
	TransformerConfig         map[string]string    `log:"true"`
	AltNames                  map[string]string    `log:"true"`
	StoragePolicy             string               `log:"true"`
	CompactionPolicy          string               `log:"true"`
	SubNetworkID              string               `log:"true"`
	SecurityGroupIDs          []string             `log:"true"`
	Cleanup                   model.CleanupType    `log:"true"`
	DropUnknownColumns        bool                 `log:"true"`
	IsSchemaMigrationDisabled bool                 `log:"true"`
	Underlay                  bool                 `log:"true"`
	ServiceAccountID          string               `log:"true"`
	IgnoreRowTooLargeErrors   bool                 `log:"true"`
	FitDatetime               bool                 `log:"true"` // will crop date-time to allowed time range (with data-loss)
	SAKeyContent              string
	TriggingInterval          time.Duration `log:"true"`
	TriggingSize              uint64        `log:"true"`
	IsTableColumnOriented     bool          `log:"true"`
	DefaultCompression        string        `log:"true"`

	Primary bool `log:"true"` // if worker is first, i.e. primary, will run background jobs

	TLSEnabled      bool `log:"true"`
	RootCAFiles     []string
	TokenServiceURL string `log:"true"`
	UserdataAuth    bool   `log:"true"` // allow fallback to Instance metadata Auth
	OAuth2Config    *v3credential.OAuth2Config
}

var (
	_ model.Destination          = (*YdbDestination)(nil)
	_ model.AlterableDestination = (*YdbDestination)(nil)
)

func (d *YdbDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *YdbDestination) IsAlterable() {}

func (d *YdbDestination) ServiceAccountIDs() []string {
	if d.ServiceAccountID != "" {
		return []string{d.ServiceAccountID}
	}
	return nil
}

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
