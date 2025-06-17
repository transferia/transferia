//go:build !disable_delta_provider

package delta

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
)

// To verify providers contract implementation
var (
	_ model.Source = (*DeltaSource)(nil)
)

type DeltaSource struct {
	Bucket           string
	AccessKey        string
	S3ForcePathStyle bool
	SecretKey        model.SecretString
	PathPrefix       string
	Endpoint         string
	UseSSL           bool
	VersifySSL       bool
	Region           string

	HideSystemCols bool // to hide system cols `__delta_file_name` and `__delta_row_index` cols from out struct

	// delta lake hold always single table, and TableID of such table defined by user
	TableName      string
	TableNamespace string
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
