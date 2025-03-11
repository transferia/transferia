package instanceutil

import (
	"fmt"
	"io"
	"net/http"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"gopkg.in/yaml.v2"
)

const InstanceMetadataAddr = "169.254.169.254"

type GoogleCEMetaDataParam string

const (
	GoogleAllParams         = GoogleCEMetaDataParam("")
	GoogleIdentityRSA       = GoogleCEMetaDataParam("vendor/identity/rsa")
	GoogleAttributes        = GoogleCEMetaDataParam("attributes/")
	GoogleUserData          = GoogleCEMetaDataParam("attributes/user-data")
	GoogleSSHKeys           = GoogleCEMetaDataParam("attributes/ssh-keys")
	GoogleDescription       = GoogleCEMetaDataParam("description")
	GoogleDisks             = GoogleCEMetaDataParam("disks/")
	GoogleHostname          = GoogleCEMetaDataParam("hostname")
	GoogleID                = GoogleCEMetaDataParam("id")
	GoogleName              = GoogleCEMetaDataParam("name")
	GoogleNetworkInterfaces = GoogleCEMetaDataParam("networkInterfaces/")
	GoogleServiceAccounts   = GoogleCEMetaDataParam("service-accounts")
	GoogleSADefaultToken    = GoogleCEMetaDataParam("service-accounts/default/token")
)

func GetGoogleCEMetaData(param GoogleCEMetaDataParam, recursive bool) (string, error) {
	request, err := makeGoogleMetadataRequest(param, recursive)
	if err != nil {
		return "", xerrors.Errorf("cannot create request: %w", err)
	}
	return doRequest(request)
}

func GetGoogleCEUserData(out interface{}) error {
	value, err := GetGoogleCEMetaData(GoogleUserData, false)
	if err != nil {
		return xerrors.Errorf("cannot get user data: %w", err)
	}
	return yaml.Unmarshal([]byte(value), out)
}

func GetIdentityDocument() (string, error) {
	return GetGoogleCEMetaData(GoogleIdentityRSA, false)
}

func makeGoogleMetadataRequest(param GoogleCEMetaDataParam, recursive bool) (*http.Request, error) {
	url := fmt.Sprintf("http://%v/computeMetadata/v1/instance/%v?recursive=%v", InstanceMetadataAddr, param, recursive)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Metadata-Flavor", "Google")
	return request, nil
}

func doRequest(request *http.Request) (string, error) {
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return "", xerrors.Errorf("got not OK response status %v", response.StatusCode)
	}

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return "", xerrors.Errorf("cannot read response body: %w", err)
	}
	return string(bodyBytes), nil
}

type AmazonEC2MetaDataParam string

const (
	AmazonAllParams     = AmazonEC2MetaDataParam("")
	AmazonHostname      = AmazonEC2MetaDataParam("hostname")
	AmazonID            = AmazonEC2MetaDataParam("instance-id")
	AmazonLocalIPv4     = AmazonEC2MetaDataParam("local-ipv4")
	AmazonLocalHostname = AmazonEC2MetaDataParam("local-hostname")
	AmazonMAC           = AmazonEC2MetaDataParam("mac")
	AmazonPublicIPv4    = AmazonEC2MetaDataParam("public-ipv4")
)

func GetAmazonEC2MetaData(param AmazonEC2MetaDataParam) (string, error) {
	url := fmt.Sprintf("http://%v/latest/meta-data/%v", InstanceMetadataAddr, param)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", xerrors.Errorf("cannot get metadata: %w", err)
	}
	return doRequest(request)
}

func GetAmazonEC2UserData(out interface{}) error {
	// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-add-user-data.html
	url := fmt.Sprintf("http://%v/latest/user-data", InstanceMetadataAddr)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return xerrors.Errorf("cannot make request for user data: %w", err)
	}
	value, err := doRequest(request)
	if err != nil {
		return xerrors.Errorf("cannot get user data: %w", err)
	}
	return yaml.Unmarshal([]byte(value), out)
}
