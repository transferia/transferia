package yc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	iampb "github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/transferia/transferia/cloud/dataplatform/appconfig"
	"github.com/transferia/transferia/library/go/core/xerrors"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// go-sumtype:decl CredsConfig
type CredsConfig interface {
	appconfig.TypeTagged
	isCloudCreds()
}

type PassportOauthToken struct {
	Token appconfig.Secret `mapstructure:"token"`
}

type SAKey struct {
	ID               string           `mapstructure:"id" json:"id"`
	ServiceAccountID string           `mapstructure:"service_account_id" json:"service_account_id"`
	KeyAlgorithm     string           `mapstructure:"key_algorithm" json:"key_algorithm"`
	PublicKey        string           `mapstructure:"public_key" json:"public_key"`
	PrivateKey       appconfig.Secret `mapstructure:"private_key" json:"private_key"`
}

type InstanceMetadata struct{}

type ServiceAccountKeyFile struct {
	FileName string `mapstructure:"file_name"`
}

type ConstantIAMToken struct {
	Token appconfig.Secret `mapstructure:"token"`
}

type YcProfile struct {
	ProfileName string `mapstructure:"profile_name" yaml:"profile_name"`
}

func (t *YcProfile) YandexCloudAPICredentials() {}
func (t *YcProfile) IAMToken(ctx context.Context) (*iampb.CreateIamTokenResponse, error) {
	command := exec.Command("yc", "iam", "create-token", "--profile", t.ProfileName)
	token, err := command.Output()
	if err != nil {
		return nil, xerrors.Errorf("unable to get cli output: %w", err)
	}

	iamToken := strings.TrimSuffix(string(token), "\n")
	return &iampb.CreateIamTokenResponse{
		IamToken:  iamToken,
		ExpiresAt: &timestamppb.Timestamp{Seconds: time.Now().Add(time.Hour * 12).Unix()},
	}, nil
}

func (t *YcProfile) APIEndpoint() (string, error) {
	command := exec.Command("yc", "config", "get", "endpoint", "--profile", t.ProfileName)
	endpoint, err := command.Output()
	if err != nil {
		return "", xerrors.Errorf("unable to get cli output: %w", err)
	}
	return string(endpoint), nil
}

type YcpProfile struct {
	ProfileName string `mapstructure:"profile_name" yaml:"profile_name"`
}

func (t *YcpProfile) YandexCloudAPICredentials() {}
func (t *YcpProfile) IAMToken(ctx context.Context) (*iampb.CreateIamTokenResponse, error) {
	command := exec.Command("ycp", "iam", "create-token", "--profile", t.ProfileName)
	token, err := command.Output()
	if err != nil {
		return nil, xerrors.Errorf("unable to get cli output: %w", err)
	}

	iamToken := strings.TrimSuffix(string(token), "\n")
	return &iampb.CreateIamTokenResponse{
		IamToken:  iamToken,
		ExpiresAt: &timestamppb.Timestamp{Seconds: time.Now().Add(time.Hour * 12).Unix()},
	}, nil
}

type YcpServiceAccount struct {
	ProfileName string `mapstructure:"profile_name"`
	AccountId   string `mapstructure:"account_id"`
}

func (t *YcpServiceAccount) YandexCloudAPICredentials() {}
func (t *YcpServiceAccount) IAMToken(ctx context.Context) (*iampb.CreateIamTokenResponse, error) {
	command := exec.Command("ycp", "--profile", t.ProfileName, "iam", "v1", "iam-token", "create-for-service-account", "--service-account-id", t.AccountId)
	token, err := command.Output()
	if err != nil {
		return nil, xerrors.Errorf("unable to get cli output: %w", err)
	}

	iamToken := strings.TrimSuffix(string(token), "\n")
	return &iampb.CreateIamTokenResponse{
		IamToken:  iamToken,
		ExpiresAt: &timestamppb.Timestamp{Seconds: time.Now().Add(time.Hour * 12).Unix()},
	}, nil
}

func (*PassportOauthToken) isCloudCreds() {}
func (*PassportOauthToken) IsTypeTagged() {}

func (*SAKey) isCloudCreds() {}
func (*SAKey) IsTypeTagged() {}

func (*InstanceMetadata) isCloudCreds() {}
func (*InstanceMetadata) IsTypeTagged() {}

func (*ServiceAccountKeyFile) isCloudCreds() {}
func (*ServiceAccountKeyFile) IsTypeTagged() {}

func (*ConstantIAMToken) isCloudCreds() {}
func (*ConstantIAMToken) IsTypeTagged() {}

func (*YcProfile) isCloudCreds() {}
func (*YcProfile) IsTypeTagged() {}

func (*YcpProfile) isCloudCreds() {}
func (*YcpProfile) IsTypeTagged() {}

func (*YcpServiceAccount) isCloudCreds() {}
func (*YcpServiceAccount) IsTypeTagged() {}

func (t *ConstantIAMToken) YandexCloudAPICredentials() {}
func (t *ConstantIAMToken) IAMToken(ctx context.Context) (*iampb.CreateIamTokenResponse, error) {
	return &iampb.CreateIamTokenResponse{
		IamToken:  string(t.Token),
		ExpiresAt: &timestamppb.Timestamp{Seconds: time.Now().Add(time.Hour * 12).Unix()},
	}, nil
}

func init() {
	appconfig.RegisterTypeTagged((*CredsConfig)(nil), (*PassportOauthToken)(nil), "passport_oauth_token", nil)
	appconfig.RegisterTypeTagged((*CredsConfig)(nil), (*SAKey)(nil), "service_account_key", nil)
	appconfig.RegisterTypeTagged((*CredsConfig)(nil), (*InstanceMetadata)(nil), "instance_metadata", nil)
	appconfig.RegisterTypeTagged((*CredsConfig)(nil), (*YcProfile)(nil), "yc_profile", nil)
	appconfig.RegisterTypeTagged((*CredsConfig)(nil), (*YcpProfile)(nil), "ycp_profile", nil)
	appconfig.RegisterTypeTagged((*CredsConfig)(nil), (*YcpServiceAccount)(nil), "ycp_service_account", nil)
	appconfig.RegisterTypeTagged((*CredsConfig)(nil), (*ServiceAccountKeyFile)(nil), "service_account_key_file", nil)
	appconfig.RegisterTypeTagged((*CredsConfig)(nil), (*ConstantIAMToken)(nil), "iam_token", nil)
}

func CredentialsFromConfig(creds CredsConfig) (Credentials, error) {
	var ycCreds Credentials
	switch configuredCreds := creds.(type) {
	case *ServiceAccountKeyFile:
		fileName := configuredCreds.FileName
		fileContent, err := os.ReadFile(fileName)
		if err != nil {
			return nil, xerrors.Errorf(`Can't read file "%s" for credentials`, fileName)
		}
		creds := new(SAKey)
		if err := json.Unmarshal([]byte(fileContent), &creds); err != nil {
			return nil, xerrors.Errorf(`Can't unmarshal file "%s" for credentials`, fileName)
		}
		iamKey := iam.Key{
			Id:           creds.ID,
			Subject:      &iam.Key_ServiceAccountId{ServiceAccountId: creds.ServiceAccountID},
			Description:  fmt.Sprintf("key for service account: %s", creds.ServiceAccountID),
			KeyAlgorithm: iam.Key_Algorithm(iam.Key_Algorithm_value[creds.KeyAlgorithm]),
			PublicKey:    creds.PublicKey,
		}
		ycCreds, err = ServiceAccountKey(&iamKey, string(creds.PrivateKey))
		if err != nil {
			return nil, xerrors.Errorf("Cannot create service account key: %w", err)
		}
	case *YcpProfile:
		ycCreds = configuredCreds
	case *YcpServiceAccount:
		ycCreds = configuredCreds
	case *YcProfile:
		ycCreds = configuredCreds
	case *InstanceMetadata:
		ycCreds = InstanceServiceAccount()
	case *PassportOauthToken:
		ycCreds = OAuthToken(string(configuredCreds.Token))
	case *SAKey:
		iamKey := iam.Key{
			Id:           configuredCreds.ID,
			Subject:      &iam.Key_ServiceAccountId{ServiceAccountId: configuredCreds.ServiceAccountID},
			Description:  fmt.Sprintf("key for service account: %s", configuredCreds.ServiceAccountID),
			KeyAlgorithm: iam.Key_Algorithm(iam.Key_Algorithm_value[configuredCreds.KeyAlgorithm]),
			PublicKey:    configuredCreds.PublicKey,
		}
		var err error
		ycCreds, err = ServiceAccountKey(&iamKey, string(configuredCreds.PrivateKey))
		if err != nil {
			return nil, xerrors.Errorf("Cannot create service account key: %w", err)
		}
	case *ConstantIAMToken:
		ycCreds = configuredCreds
	}
	if ycCreds == nil {
		return nil, xerrors.Errorf("Unknown credentials type: %T", creds)
	}
	return ycCreds, nil
}
