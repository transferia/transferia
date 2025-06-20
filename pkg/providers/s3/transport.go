//go:build !disable_s3_provider

package s3

import (
	"context"
	"net/http"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/credentials"
)

const (
	XYaCloudTokenHeader string = "X-YaCloud-SubjectToken"
	tokenGetTimeout            = 10 * time.Second
)

type withCredentialsRoundTripper struct {
	credentials credentials.Credentials
	wrapped     http.RoundTripper
}

// newCredentialsRoundTripper constructs a round-tripper which inserts a YC header with a YC token into each request.
// This is against the requirement of http.RoundTripper, but it works.
func newCredentialsRoundTripper(credentials credentials.Credentials, wrapped http.RoundTripper) *withCredentialsRoundTripper {
	return &withCredentialsRoundTripper{
		credentials: credentials,
		wrapped:     wrapped,
	}
}

func (t *withCredentialsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(req.Header.Get(XYaCloudTokenHeader)) == 0 {
		tokenGetCtx, cancel := context.WithTimeout(context.Background(), tokenGetTimeout)
		defer cancel()
		token, err := t.credentials.Token(tokenGetCtx)
		if err != nil {
			return nil, xerrors.Errorf("failed to get token: %w", err)
		}
		req.Header.Set(XYaCloudTokenHeader, token)
	}

	result, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return result, xerrors.Errorf("failed to execute RoundTrip with %q header set: %w", XYaCloudTokenHeader, err)
	}
	return result, nil
}
