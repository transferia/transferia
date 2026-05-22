package googlemetadata

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

const InstanceMetadataAddr = "169.254.169.254"

type GoogleCEMetaDataParam string

func (p GoogleCEMetaDataParam) String() string {
	return string(p)
}

func (p GoogleCEMetaDataParam) Child(name string) GoogleCEMetaDataParam {
	base := strings.TrimSuffix(string(p), "/")
	name = strings.TrimPrefix(name, "/")
	return GoogleCEMetaDataParam(base + "/" + name)
}

func MetadataPath(path string) GoogleCEMetaDataParam {
	return GoogleCEMetaDataParam(path)
}

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

func GetMetadataIntField(field GoogleCEMetaDataParam) (int, error) {
	rawValue, err := GetMetadataRawField(field)
	if err != nil {
		return 0, xerrors.Errorf("cannot get field %q: %w", field, err)
	}
	if value, ok := rawValue.(int); !ok {
		return 0, xerrors.Errorf("value of field %q is not int", field)
	} else {
		return value, nil
	}
}

func GetMetadataBoolField(field GoogleCEMetaDataParam) (bool, error) {
	rawValue, err := GetMetadataRawField(field)
	if err != nil {
		return false, xerrors.Errorf("cannot get field %q: %w", field, err)
	}
	if value, ok := rawValue.(bool); !ok {
		return false, xerrors.Errorf("value of field %q is not bool", field)
	} else {
		return value, nil
	}
}

func GetMetadataStringField(field GoogleCEMetaDataParam) (string, error) {
	rawValue, err := GetMetadataRawField(field)
	if err != nil {
		return "", xerrors.Errorf("cannot get field %q: %w", field, err)
	}
	if value, ok := rawValue.(string); !ok {
		return "", xerrors.Errorf("value of field %q is not string", field)
	} else {
		return value, nil
	}
}

func GetMetadataRawField(field GoogleCEMetaDataParam) (interface{}, error) {
	request, err := makeGoogleMetadataRequest(field, false)
	if err != nil {
		return nil, xerrors.Errorf("cannot create request: %w", err)
	}
	return doRequest(request)
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
