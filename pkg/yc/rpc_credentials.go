package yc

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/token"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
)

type rpcCredentials struct {
	plaintext bool

	createToken createIAMTokenFunc // Injected on Init
	// now may be replaced in tests
	now func() time.Time

	// mutex guards conn and currentState, and excludes multiple simultaneous token updates
	mutex        sync.RWMutex
	currentState rpcCredentialsState
}

var _ credentials.PerRPCCredentials = new(rpcCredentials)

type rpcCredentialsState struct {
	token     string
	expiresAt time.Time
	version   int64
}

func newRPCCredentials(plaintext bool) *rpcCredentials {
	var credentialState rpcCredentialsState
	return &rpcCredentials{
		plaintext:    plaintext,
		createToken:  nil,
		now:          time.Now,
		mutex:        sync.RWMutex{},
		currentState: credentialState,
	}
}

type createIAMTokenFunc func(ctx context.Context) (*iam.CreateIamTokenResponse, error)

func (c *rpcCredentials) Init(createToken createIAMTokenFunc) {
	c.createToken = createToken
}

func (c *rpcCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	audienceURL, err := url.Parse(uri[0])
	if err != nil {
		return nil, xerrors.Errorf("cannot parse URI %s: %w", uri[0], err)
	}
	if audienceURL.Path == "/yandex.cloud.iam.v1.IamTokenService" ||
		audienceURL.Path == "/yandex.cloud.endpoint.ApiEndpointService" ||
		audienceURL.Path == "/yandex.cloud.priv.iam.v1.IamTokenService" {
		return nil, nil
	}

	if IsWithUserAuth(ctx) {
		tkn, ok := token.FromContext(ctx)
		if !ok {
			return nil, xerrors.New(token.ErrUnableToGetToken)
		}
		return newAuthMetadata(tkn), nil
	}

	c.mutex.RLock()
	state := c.currentState
	c.mutex.RUnlock()

	token := state.token
	expired := c.now().After(state.expiresAt)
	if expired {
		token, err = c.updateToken(ctx, state)
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Unauthenticated {
				return nil, err
			}
			return nil, status.Errorf(codes.Unauthenticated, "%v", err)
		}
	}

	return newAuthMetadata(token), nil
}

func newAuthMetadata(token string) map[string]string {
	return map[string]string{
		"authorization": "Bearer " + token,
	}
}

func (c *rpcCredentials) RequireTransportSecurity() bool {
	return !c.plaintext
}

func (c *rpcCredentials) updateToken(ctx context.Context, currentState rpcCredentialsState) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.currentState.version != currentState.version {
		// someone have already updated it
		return c.currentState.token, nil
	}

	resp, err := c.createToken(ctx)
	if err != nil {
		return "", err
	}
	expiresAt := resp.ExpiresAt.AsTime()
	if expiresAtErr := resp.ExpiresAt.CheckValid(); expiresAtErr != nil {
		grpclog.Warningf("invalid IAM Token expires_at: %s", expiresAtErr)
		// Fallback to short term caching.
		expiresAt = time.Now().Add(time.Minute)
	}
	c.currentState = rpcCredentialsState{
		token:     resp.IamToken,
		expiresAt: expiresAt,
		version:   currentState.version + 1,
	}
	return c.currentState.token, nil
}
