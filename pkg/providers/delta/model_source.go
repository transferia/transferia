package delta

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	"go.uber.org/zap/zapcore"
)

// To verify providers contract implementation
var (
	_ model.Source = (*DeltaSource)(nil)
)

type DeltaSource struct {
	Bucket           string `log:"true"`
	AccessKey        string
	S3ForcePathStyle bool `log:"true"`
	SecretKey        model.SecretString
	PathPrefix       string `log:"true"`
	Endpoint         string `log:"true"`
	UseSSL           bool   `log:"true"`
	VersifySSL       bool   `log:"true"`
	Region           string `log:"true"`

	HideSystemCols bool `log:"true"` // to hide system cols `__delta_file_name` and `__delta_row_index` cols from out struct

	// delta lake hold always single table, and TableID of such table defined by user
	TableName      string `log:"true"`
	TableNamespace string `log:"true"`
}

func (d *DeltaSource) ConnectionConfig() s3_provider.ConnectionConfig {
	return s3_provider.ConnectionConfig{
		AccessKey:        d.AccessKey,
		S3ForcePathStyle: d.S3ForcePathStyle,
		SecretKey:        d.SecretKey,
		Endpoint:         d.Endpoint,
		UseSSL:           d.UseSSL,
		VerifySSL:        d.VersifySSL,
		Region:           d.Region,
		ServiceAccountID: "",
	}
}

func (d *DeltaSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *DeltaSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *DeltaSource) Validate() error {
	return nil
}

func (d *DeltaSource) WithDefaults() {
}

func (d *DeltaSource) IsSource() {
}
