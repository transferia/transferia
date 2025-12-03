package ydb

import (
	"github.com/transferia/transferia/pkg/abstract/model"
	v3credential "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

type YdbStorageParams struct {
	Database           string             `log:"true"`
	Instance           string             `log:"true"`
	Tables             []string           `log:"true"`
	TableColumnsFilter []YdbColumnsFilter `log:"true"`
	UseFullPaths       bool               `log:"true"`

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
